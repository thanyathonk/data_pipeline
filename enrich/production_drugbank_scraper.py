import os, time, json, argparse, random, pandas as pd, pickle, shutil
from urllib.parse import quote_plus
try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import undetected_chromedriver as uc
    HAVE_SELENIUM = True
except Exception:
    # Selenium stack not available in demo/testing environments
    By = WebDriverWait = EC = uc = None  # type: ignore
    HAVE_SELENIUM = False
from threading import Lock
from datetime import datetime

# ---------- Defaults (override by CLI) ----------
INPUT_FILE = "data/enrich/drug_rxnorm.csv.gz"
OUTPUT_DIR = "data/enrich/drugbank"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
BATCH_SIZE = 50
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 10
MAX_BATCHES = None
ITEM_CHECKPOINT_EVERY = 10
PROFILE_DIR = os.path.join(OUTPUT_DIR, "chrome_profile")
INTERACTIVE_INIT = False
INIT_VISIBLE = False
ALLOW_DUP_DBID = True

results_lock = Lock()
session_driver = None
session_ready = False


class ProductionScraper:
    def __init__(self):
        self.cache_file = os.path.join(CACHE_DIR, "drugbank_cache.pkl")
        self.progress_file = os.path.join(OUTPUT_DIR, "progress.json")
        self.results_file = os.path.join(OUTPUT_DIR, "drugbank_results.csv")
        self.error_log = os.path.join(OUTPUT_DIR, "error_log.txt")
        self.done_ok = os.path.join(OUTPUT_DIR, "done.ok")
        self.profile_dir = PROFILE_DIR
        self.interactive_init = INTERACTIVE_INIT
        self.init_visible = INIT_VISIBLE

        # Load cache and progress
        self.cache = self.load_cache()
        self.progress = self.load_progress()

        print(f"🚀 Production DrugBank Scraper Initialized")
        print(f"📁 Cache: {len(self.cache)} entries")
        print(f"📊 Progress: Batch {self.progress.get('current_batch', 0)}")

    def load_cache(self):
        """Load existing cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "rb") as f:
                    cache = pickle.load(f)
                print(f"✅ Loaded cache: {len(cache)} entries")
                return cache
        except Exception as e:
            print(f"⚠️  Cache load error: {e}")
        return {}

    def save_cache(self):
        """Save cache to disk"""
        try:
            with open(self.cache_file, "wb") as f:
                pickle.dump(self.cache, f)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def load_progress(self):
        """Load progress from disk"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, "r") as f:
                    progress = json.load(f)
                print(f"✅ Loaded progress: {progress}")
                return progress
        except Exception as e:
            print(f"⚠️  Progress load error: {e}")
        return {"current_batch": 0, "completed_ingredients": 0, "total_ingredients": 0}

    def save_progress(self, batch_num, completed, total):
        """Save progress to disk"""
        progress = {
            "current_batch": batch_num,
            "completed_ingredients": completed,
            "total_ingredients": total,
            "last_updated": datetime.now().isoformat(),
        }
        try:
            with open(self.progress_file, "w") as f:
                json.dump(progress, f, indent=2)
            self.progress = progress
        except Exception as e:
            print(f"❌ Progress save error: {e}")

    def write_done(self, reason: str):
        try:
            with open(self.done_ok, "w") as f:
                f.write(f"{datetime.now().isoformat()} | {reason}\n")
        except Exception as e:
            print(f"⚠️  Unable to write done marker: {e}")

    def log_error(self, ingredient, error_msg):
        """Log errors to file"""
        try:
            with open(self.error_log, "a") as f:
                timestamp = datetime.now().isoformat()
                f.write(f"{timestamp}: {ingredient} - {error_msg}\n")
        except Exception as e:
            print(f"❌ Error log failed: {e}")

    def _resolve_chrome_binary(self) -> str:
        """
        ค้นหา Chrome/Chromium ที่ติดตั้งในเครื่องผู้ใช้ (ต้องมีอยู่แล้ว)
        ลำดับความสำคัญ: --chrome-binary > CHROME_BIN > PATH > พาธยอดนิยม
        """
        cands = []
        cb = getattr(self, "chrome_binary", None)
        if cb:
            cands.append(cb)
        env_cb = os.getenv("CHROME_BIN")
        if env_cb:
            cands.append(env_cb)
        for exe in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser"):
            p = shutil.which(exe)
            if p:
                cands.append(p)
        cands += [
            "/opt/google/chrome/google-chrome",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        ]
        for p in cands:
            if isinstance(p, str) and os.path.isfile(p) and os.access(p, os.X_OK):
                return p
        raise SystemExit("[drugbank] ไม่พบ Chrome/Chromium ในเครื่อง โปรดติดตั้ง หรือกำหนด CHROME_BIN/--chrome-binary")

    def init_session(self):
        """Initialize DrugBank session"""
        global session_driver, session_ready

        if session_ready:
            return True

        print("🔥 Initializing DrugBank session...")

        # Build options freshly in each attempt

        last_err = None
        for attempt in range(1, 4):
            try:
                options = uc.ChromeOptions()
                options.add_argument("--window-size=1200,900")
                # Persist profile to retain Cloudflare cookies between runs
                os.makedirs(self.profile_dir, exist_ok=True)
                options.add_argument(f"--user-data-dir={self.profile_dir}")
                # ใช้ non-headless ช่วง init; ถ้าไม่ให้แสดงหน้าต่างให้ย้ายออกนอกจอ
                if not self.init_visible:
                    options.add_argument("--window-position=-2000,-2000")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--disable-extensions")
                options.add_argument("--disable-web-security")
                options.add_argument("--disable-plugins")
                options.add_argument("--disable-notifications")
                options.add_argument("--disable-popup-blocking")
                options.add_argument("--disable-blink-features=AutomationControlled")

                # Additional stability options
                options.add_argument("--max_old_space_size=4096")
                options.add_argument("--disable-background-timer-throttling")
                options.add_argument("--disable-renderer-backgrounding")
                options.add_argument("--disable-backgrounding-occluded-windows")
                options.add_argument("--disable-ipc-flooding-protection")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")

                prefs = {
                    "profile.managed_default_content_settings.images": 2,
                    "profile.default_content_setting_values.notifications": 2,
                    "profile.managed_default_content_settings.media_stream": 2,
                }
                options.add_experimental_option("prefs", prefs)

                options.add_argument(
                    "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                # กำหนด Chrome binary (ต้องมีในเครื่องผู้ใช้)
                options.binary_location = self._resolve_chrome_binary()
                # Faster navigation returns; we will wait for elements explicitly
                try:
                    options.page_load_strategy = "eager"
                except Exception:
                    pass
                # ใช้ uc.Chrome (ต้องมี Chrome ติดตั้งไว้แล้ว)
                session_driver = uc.Chrome(options=options)

                # Set timeouts for better stability
                session_driver.implicitly_wait(1)
                session_driver.set_page_load_timeout(90)
                session_driver.set_script_timeout(60)
                try:
                    # ลด HTTP read timeout ของตัว command executor ลง เพื่อไม่ให้รอ 120s ทุกครั้ง
                    session_driver.command_executor.set_timeout(30)
                except Exception:
                    pass

                # Navigate and bypass Cloudflare with retry (ยิงไปหน้า search results ตรงๆ)
                nav_attempts = 0
                while nav_attempts < 3:
                    try:
                        session_driver.get(
                            "https://go.drugbank.com/unearth/q?query=aspirin&searcher=drugs"
                        )
                        break
                    except Exception as nav_err:
                        nav_attempts += 1
                        print(f"⚠️  Navigation attempt {nav_attempts} failed: {nav_err}")
                        if nav_attempts >= 3:
                            raise
                        time.sleep(3)

                # Optional interactive pause to allow manual Cloudflare verification
                if getattr(self, "interactive_init", False):
                    print("👋 Interactive init: Chrome window should be visible.")
                    print(
                        "   Please complete any Cloudflare/human verification until search results show,"
                    )
                    print(
                        "   then return to this terminal and press Enter to continue…"
                    )
                    try:
                        input("   Press Enter to continue once ready … ")
                    except Exception:
                        # If no TTY available, continue without blocking
                        print(
                            "   (No interactive TTY detected; continuing without pause)"
                        )

                start_time = time.time()
                last_heartbeat = start_time
                while time.time() - start_time < 90:
                    try:
                        # รอให้ลิงก์ผลลัพธ์ปรากฏยืนยันว่า session พร้อม
                        _ = WebDriverWait(session_driver, 5).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "a[href^='/drugs/']")
                            )
                        )
                        session_ready = True
                        bypass_time = time.time() - start_time
                        print(f"✅ Session ready in {bypass_time:.1f}s")
                        return True
                    except Exception:
                        now = time.time()
                        if now - last_heartbeat >= 10:
                            print("   …still waiting for search results…")
                            last_heartbeat = now
                        time.sleep(1)

                print("❌ Session initialization failed (timeout)")
                # Clean up this driver and retry fresh
                try:
                    session_driver.quit()
                except Exception:
                    pass
                session_driver = None
                last_err = TimeoutError("Init timeout waiting for search box")
            except Exception as e:
                print(f"❌ Session error (attempt {attempt}/3): {e}")
                last_err = e
                # Clean up before next retry
                try:
                    if session_driver:
                        session_driver.quit()
                except Exception:
                    pass
                session_driver = None
                time.sleep(5)

        print(f"❌ Failed to initialize session after retries: {last_err}")
        return False

    def is_session_valid(self):
        """Check if current session is still valid and on correct page"""
        global session_driver, session_ready

        if not session_ready or not session_driver:
            return False

        try:
            # Check if session is alive
            current_url = session_driver.current_url

            # Check if we're on DrugBank domain
            if "drugbank.com" not in current_url:
                return False

            # Try to find search box to ensure page is loaded correctly
            search_box = session_driver.find_element(By.NAME, "query")

            # Additional check: ensure we can interact with the page
            session_driver.execute_script("return document.readyState") == "complete"

            return True
        except Exception:
            return False

    def cleanup_session(self):
        """Clean up current session"""
        global session_driver, session_ready
        if session_driver:
            try:
                session_driver.quit()
            except:
                pass
            session_driver = None
            session_ready = False

    def extract_drugbank_data(self, ingredient):
        """Extract data from DrugBank with error handling and session recovery"""
        global session_driver, session_ready

        # Check cache first
        if ingredient in self.cache:
            return self.cache[ingredient]

        retries = 0
        while retries < MAX_RETRIES:
            # Check if session is still valid, if not reinitialize
            if not session_ready or not self.is_session_valid():
                print(f"   🔄 Session invalid, reinitializing...")
                self.cleanup_session()
                if not self.init_session():
                    return {
                        "ingredient": ingredient,
                        "status": "failed",
                        "error": "Session initialization failed",
                    }
            try:
                # Fast search: go directly to search results page (skips home typing)
                search_url = f"https://go.drugbank.com/unearth/q?query={quote_plus(ingredient)}&searcher=drugs"
                session_driver.get(search_url)

                # Navigate to drug page
                current_url = session_driver.current_url
                if "/drugs/DB" not in current_url:
                    try:
                        first_link = WebDriverWait(session_driver, 8).until(
                            EC.element_to_be_clickable(
                                (By.CSS_SELECTOR, "a[href^='/drugs/']")
                            )
                        )
                        session_driver.execute_script(
                            "arguments[0].click();", first_link
                        )
                        time.sleep(0.3)
                    except Exception:
                        # If there is truly no result link, treat as no_results; otherwise retry
                        links = session_driver.find_elements(
                            By.CSS_SELECTOR, "a[href^='/drugs/']"
                        )
                        if not links:
                            result = {"ingredient": ingredient, "status": "no_results"}
                            self.cache[ingredient] = result
                            return result
                        else:
                            raise

                # Extract data with individual queries (more reliable)
                result = {
                    "ingredient": ingredient,
                    "status": "success",
                    "actual_drug_name": "",
                    "drugbank_id": "",
                    "modality": "",
                    "us_approved": "",
                    "other_approved": "",
                    "chemical_formula": "",
                    "unii": "",
                    "cas_number": "",
                    "inchi_key": "",
                    "inchi": "",
                    "iupac_name": "",
                    "smiles": "",
                    "average_weight": "",
                    "monoisotopic_weight": "",
                }

                # Get drug name
                try:
                    h1 = session_driver.find_element(By.TAG_NAME, "h1")
                    result["actual_drug_name"] = h1.text.strip()
                except:
                    pass

                # Extract fields with individual XPath queries
                field_xpaths = {
                    "drugbank_id": "//dt[contains(text(),'DrugBank ID')]/following-sibling::dd",
                    "drugbank_accession_number": "//dt[contains(text(),'DrugBank Accession Number')]/following-sibling::dd",
                    "modality": "//dt[contains(text(),'Modality')]/following-sibling::dd",
                    "us_approved": "//dt[contains(text(),'US Approved')]/following-sibling::dd",
                    "other_approved": "//dt[contains(text(),'Other Approved')]/following-sibling::dd",
                    "chemical_formula": "//dt[contains(text(),'Chemical Formula')]/following-sibling::dd",
                    "unii": "//dt[contains(text(),'UNII')]/following-sibling::dd",
                    "cas_number": "//dt[contains(text(),'CAS')]/following-sibling::dd",
                    "inchi_key": "//dt[contains(text(),'InChI Key')]/following-sibling::dd",
                    "inchi": "//dt[text()='InChI']/following-sibling::dd",
                    "iupac_name": "//dt[contains(text(),'IUPAC')]/following-sibling::dd",
                    "smiles": "//dt[contains(text(),'SMILES')]/following-sibling::dd",
                }

                for field, xpath in field_xpaths.items():
                    try:
                        element = session_driver.find_element(By.XPATH, xpath)

                        if result["drugbank_id"] is None:
                            db_num = session_driver.find_element(
                                By.XPATH,
                                "//dt[contains(text(),'DrugBank Accession Number')]/following-sibling::dd",
                            )
                            result["drugbank_id"] = db_num.text.strip()

                        result[field] = element.text.strip()
                    except Exception:
                        pass

                # Weight data
                try:
                    weight_element = session_driver.find_element(
                        By.XPATH, "//dt[text()='Weight']/following-sibling::dd"
                    )
                    # DrugBank Accession Number

                    weight_text = weight_element.text.strip()

                    for line in weight_text.split("\n"):
                        line = line.strip()
                        if line.startswith("Average:"):
                            result["average_weight"] = line.replace(
                                "Average:", ""
                            ).strip()
                        elif line.startswith("Monoisotopic:"):
                            result["monoisotopic_weight"] = line.replace(
                                "Monoisotopic:", ""
                            ).strip()

                except:
                    pass

                # Optional duplicate DrugBank ID guard (disabled when ALLOW_DUP_DBID=True)
                if not ALLOW_DUP_DBID:
                    drugbank_id = result.get("drugbank_id", "")
                    if drugbank_id and len(drugbank_id) > 3:
                        existing_ingredients = [
                            ing
                            for ing, cached_result in self.cache.items()
                            if cached_result.get("drugbank_id") == drugbank_id
                        ]
                        if existing_ingredients:
                            print(
                                f"   ⚠️  Duplicate DrugBank ID {drugbank_id} detected between {existing_ingredients[0]} and {ingredient}"
                            )
                            # fall through without reinitializing when allowed

                # Cache the result
                self.cache[ingredient] = result
                return result

            except Exception as e:
                retries += 1
                error_msg = f"Attempt {retries}: {str(e)}"
                print(f"   ⚠️  {ingredient}: {error_msg}")
                self.log_error(ingredient, error_msg)

                # รีสตาร์ท session เร็วขึ้นเมื่อเจอ renderer/read timeout
                es = str(e)
                if (
                    "Timed out receiving message from renderer" in es
                    or "Read timed out" in es
                ):
                    self.cleanup_session()
                    if not self.init_session():
                        return {
                            "ingredient": ingredient,
                            "status": "failed",
                            "error": "Session reinit failed",
                        }

                if retries < MAX_RETRIES:
                    # backoff สั้นเพื่อลองใหม่เร็ว
                    wait_time = min(4, 1 + retries)
                    print(f"   ⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    # ครบโควตา: เก็บเป็น failed แล้วไปต่อ
                    fail_res = {
                        "ingredient": ingredient,
                        "status": "failed",
                        "error": str(e),
                    }
                    self.cache[ingredient] = fail_res
                    return fail_res

        return None

    def process_batch(
        self, ingredients_batch, batch_num, total_batches, total_ingredients=None
    ):
        """Process a batch of ingredients"""
        print(
            f"\n📦 Batch {batch_num}/{total_batches} ({len(ingredients_batch)} ingredients)"
        )

        batch_results = []
        start_time = time.time()

        for i, ingredient in enumerate(ingredients_batch):
            print(f"   🔍 {i + 1}/{len(ingredients_batch)}: {ingredient}")

            result = self.extract_drugbank_data(ingredient)
            if result:
                batch_results.append(result)

                status = "✅" if result.get("status") == "success" else "❌"
                drugbank_id = result.get("drugbank_id", "")
                modality = result.get("modality", "")
                print(f"      {status} {drugbank_id}")
                print(f"      {status} {modality}")

            # Periodic in-batch checkpoint to avoid data loss on long runs
            if (i + 1) % ITEM_CHECKPOINT_EVERY == 0:
                try:
                    self.save_cache()
                    print(f"      💾 In-batch checkpoint at item {i + 1}")
                    if total_ingredients is not None:
                        # Update progress file to reflect latest completion count
                        self.save_progress(
                            batch_num, len(self.cache), total_ingredients
                        )
                except Exception as _:
                    pass

        end_time = time.time()
        batch_time = end_time - start_time
        avg_time = batch_time / len(ingredients_batch) if ingredients_batch else 0

        print(f"   ⏱️  Batch time: {batch_time:.1f}s (avg: {avg_time:.1f}s/drug)")

        return batch_results

    def run(self):
        """Main execution function"""
        print(f"\n🚀 Starting Production DrugBank Scraper")
        print(f"📁 Input: {INPUT_FILE}")
        print(f"📊 Output: {OUTPUT_DIR}")
        completed_successfully = False

        # Load data
        print(f"\n📖 Loading data...")
        df = pd.read_csv(INPUT_FILE)

        col = getattr(self, "target_col", None)
        if not col:
            col = "ingredients" if "ingredients" in df.columns else ("INN" if "INN" in df.columns else None)
        if not col:
            raise SystemExit("Input CSV must contain 'ingredients' or 'INN' column (or specify --ingredient-col)")
        df = df.dropna(subset=[col])
        df[col] = df[col].astype(str)
        df = df[df[col] != "nan"]
        unique_ingredients = df[col].unique()
        total_ingredients = len(unique_ingredients)

        print(f"   Total rows: {len(df):,}")
        print(f"   Unique ingredients: {total_ingredients:,}")

        # Filter out already processed ingredients
        remaining_ingredients = [
            ing for ing in unique_ingredients if ing not in self.cache
        ]
        print(f"   Remaining to process: {len(remaining_ingredients):,}")

        if not remaining_ingredients:
            print("✅ All ingredients already processed!")
            # Mark completion so supervisor won't restart
            self.write_done("all_ingredients_already_processed")
            return

        # Create batches
        batches = [
            remaining_ingredients[i : i + BATCH_SIZE]
            for i in range(0, len(remaining_ingredients), BATCH_SIZE)
        ]
        total_batches = len(batches)

        print(f"   Batches: {total_batches} (size: {BATCH_SIZE})")

        # Initialize session
        if not self.init_session():
            print("❌ Failed to initialize session")
            return

        try:
            all_results = []
            start_batch = self.progress.get("current_batch", 0)

            # If cache is empty but progress indicates prior batches, reset progress
            if start_batch > 0 and len(self.cache) == 0:
                print(
                    "⚠️  Progress indicates prior batches but cache is empty; resetting progress to 0"
                )
                start_batch = 0
                self.save_progress(0, 0, total_ingredients)

            for batch_num, batch in enumerate(batches[start_batch:], start_batch + 1):
                # Process batch
                batch_results = self.process_batch(
                    batch, batch_num, total_batches, total_ingredients
                )
                all_results.extend(batch_results)

                # Save progress
                completed = len(self.cache)
                self.save_progress(batch_num, completed, total_ingredients)

                # Save cache periodically
                if batch_num % CHECKPOINT_INTERVAL == 0:
                    print(f"💾 Saving checkpoint...")
                    self.save_cache()

                    # Save intermediate results
                    if all_results:
                        temp_df = pd.DataFrame(list(self.cache.values()))
                        temp_file = os.path.join(
                            OUTPUT_DIR, f"temp_results_batch_{batch_num}.csv"
                        )
                        temp_df.to_csv(temp_file, index=False)
                        print(f"   💾 Temp results: {temp_file}")

                # Respect MAX_BATCHES limit relative to start_batch
                if MAX_BATCHES is not None and (batch_num - start_batch) >= MAX_BATCHES:
                    print(
                        f"\n⏹️  Reached max batches limit: {MAX_BATCHES}. Stopping early."
                    )
                    # Mark planned early stop
                    self.write_done(f"reached_max_batches_{MAX_BATCHES}")
                    break

            # Final save
            print(f"\n💾 Saving final results...")
            self.save_cache()

            # Create final results file
            if self.cache:
                final_df = pd.DataFrame(list(self.cache.values()))
                final_df.to_csv(self.results_file, index=False)

                # Statistics
                success_count = len(
                    [r for r in self.cache.values() if r.get("status") == "success"]
                )
                print(f"\n📊 FINAL RESULTS:")
                print(f"   ✅ Success: {success_count:,}")
                print(f"   ❌ Failed: {len(self.cache) - success_count:,}")
                print(f"   📁 Results: {self.results_file}")
            completed_successfully = True

        finally:
            self.cleanup_session()
            # If completed successfully, ensure done marker exists
            if completed_successfully:
                try:
                    if not os.path.exists(self.done_ok):
                        self.write_done("completed_successfully")
                except Exception:
                    pass


def main():
    global MAX_BATCHES, BATCH_SIZE, PROFILE_DIR, INTERACTIVE_INIT, INIT_VISIBLE, OUTPUT_DIR, CACHE_DIR, INPUT_FILE

    parser = argparse.ArgumentParser(description="Production DrugBank Scraper")
    parser.add_argument("--input-file", type=str, default=INPUT_FILE,
                        help="Input CSV (default: data/enrich/drug_rxnorm.csv.gz)")
    parser.add_argument("--ingredient-col", type=str, default=None,
                        help="Column containing ingredient names (default: auto pick ingredients/INN)")
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--profile-dir", type=str, default=None)
    parser.add_argument("--interactive-init", action="store_true")
    parser.add_argument("--init-visible", action="store_true")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--chrome-binary", type=str, default=None,
                        help="Path to Chrome/Chromium binary (or set CHROME_BIN env)")
    parser.add_argument("--demo", action="store_true", help="Run in demo mode (no browser); synthesize DrugBank IDs for sample testing")
    args = parser.parse_args()

    if args.max_batches is not None and args.max_batches > 0:
        MAX_BATCHES = args.max_batches
    if args.batch_size is not None and args.batch_size > 0:
        BATCH_SIZE = args.batch_size
    if args.profile_dir:
        PROFILE_DIR = args.profile_dir
    if args.interactive_init:
        INTERACTIVE_INIT = True
    if args.init_visible:
        INIT_VISIBLE = True
    if args.output_dir:
        OUTPUT_DIR = args.output_dir
    if args.input_file:
        INPUT_FILE = args.input_file

    global session_driver, session_ready
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
    os.makedirs(CACHE_DIR, exist_ok=True)

    # DEMO path: generate a minimal mapping file deterministically without network/browser
    if args.demo:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        inp = pd.read_csv(INPUT_FILE)
        # choose ingredient column: prefer INN, else ingredient, else clean_name
        col = None
        for c in ("INN","ingredient","clean_name"):
            if c in inp.columns:
                col = c; break
        if not col:
            raise SystemExit("[drugbank demo] Cannot find ingredient column (INN/ingredient/clean_name)")
        s = inp[[col]].dropna().drop_duplicates()
        s[col] = s[col].astype(str)
        # deterministic pseudo DrugBank id for demo
        def dbid(x: str) -> str:
            import hashlib
            h = hashlib.md5(x.encode("utf-8")).hexdigest()
            return "DB" + str(int(h[:6], 16) % 999999).zfill(6)
        s["drugbank_id"] = s[col].map(dbid)
        s["status"] = "success"
        s["actual_drug_name"] = s[col]
        s.rename(columns={col:"INN"}, inplace=True)
        # write results file compatible with production output
        out = os.path.join(OUTPUT_DIR, "drugbank_results.csv")
        s.to_csv(out, index=False)
        print(f"[demo] wrote {out} rows={len(s)}")
        return

    # instantiate and run real scraper
    if not HAVE_SELENIUM:
        raise SystemExit("[drugbank] Selenium stack not available. Run with --demo for smoke test or install dependencies.")
    scraper = ProductionScraper()
    if args.chrome_binary:
        scraper.chrome_binary = args.chrome_binary
    scraper.target_col = args.ingredient_col  # may be None -> auto
    scraper.run()

if __name__ == "__main__":
    main()
