# FAERS Data Pipeline (openFDA → ER → Enrich → Merge → Release)

โครงการนี้คือ pipeline สำหรับประมวลผลข้อมูลเหตุการณ์ไม่พึงประสงค์จากยา (FAERS/openFDA drug event) ตั้งแต่แปลงให้อยู่ในรูป Entity–Relationship (ER), enrich ด้วย RxNav/DrugBank, รวมผล, ตรวจสอบคุณภาพ (QA) และจัดแพ็กไฟล์สำหรับเผยแพร่ โดยมีสคริปต์ smoke test ครบชุด (Stage‑3 → Stage‑10) เพื่อช่วยยืนยันความถูกต้องของ flow บนชุดตัวอย่างขนาดเล็ก

## โครงสร้างที่ใช้บ่อย
- Stage‑2 สร้าง ER tables: `core/openFDA_Entity_Relationship_Tables_v2.py`
- Smoke test (Stage‑3 → 10): `run_smoke_test.py`
- Enrich ยา (RxNav): `enrich/rxnav_enrich.py`
- Enrich ยา (DrugBank): `enrich/production_drugbank_scraper.py`
- Split/Merge/QA/Release: `split/*.py`, `stage4_merge_back.py`, `stage5_split_cohorts.py`, `stage6_qa_checks.py`, `stage7_release_pack.py`
- Utilities ทั่วไป: `merge/common_utils.py`

## ข้อกำหนดและการเตรียมเครื่อง
### 1) Python และ virtualenv
- ใช้ Python 3.10 ขึ้นไป
- สร้าง virtualenv และติดตั้ง dependencies
```bash
bash scripts/setup_env.sh
source .venv/bin/activate
```

### 2) Vocabulary (OMOP)
- วาง vocab ที่ `vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC/`
- อย่างน้อยต้องมีไฟล์ `CONCEPT.csv` และ `CONCEPT_RELATIONSHIP.csv`

### 3) ข้อมูล openFDA (Stage‑1)
- เตรียมโครงสร้างโฟลเดอร์ `data/openFDA_drug_event/` โดยมีโฟลเดอร์ย่อยเช่น `report/`, `patient/`, `patient_drug/`, `patient_reaction/`, ...

## Stage‑2: สร้าง ER tables
รันรวดเดียว (ใช้เวลานาน) หรือรันเป็นขั้นเพื่อลดความเสี่ยง timeout
```bash
# รวดเดียว
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v

# ทีละขั้น (ตัวอย่าง)
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps report
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps patient
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps patient_drug
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps reactions
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps standard_drugs
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps standard_reactions
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps standard_drugs_atc
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps standard_drugs_rxnorm_ingredients
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v --steps standard_reactions_snomed
```
ผลลัพธ์จะอยู่ใน `data/openFDA_drug_event/er_tables/` (ไฟล์ `.csv.gz`)

## Enrich: RxNav และ DrugBank
- RxNav (ออนไลน์จริง): ใช้ค่าเริ่มต้น ไม่ต้องใส่ `--demo`
- RxNav (demo/ออฟไลน์): ใส่ `--demo` ตอนรัน `enrich/rxnav_enrich.py` หรือใช้ env `RXNAV_OFFLINE=1`
- DrugBank (จริง): ต้องมีไบนารี Chrome/Chromium (หรือใช้ Selenium Remote)
- DrugBank (demo): ใช้ `--demo` กับ `enrich/production_drugbank_scraper.py`

### เบราว์เซอร์สำหรับ DrugBank (ไม่ต้อง sudo)
วิธี A: ระบุพาธไบนารี Chrome/Chromium โดยตรง
```bash
export CHROME_BIN=/path/to/chrome  # ชี้ไปที่ไฟล์ไบนารีจริง
python run_smoke_test.py --sample-size 50 --qps 4 --max-workers 8
```
สคริปต์จะส่ง `--chrome-binary` ให้ scraper อัตโนมัติ

วิธี B: ใช้ Selenium Remote (ถ้ามี Selenium Grid)
- หากต้องการรองรับ แจ้ง URL ของ Grid เพื่อเพิ่มทางเลือกการเชื่อมต่อ

## Smoke Test (Stage‑3 → Stage‑10)
รันทดสอบครบชุดบน sample เพื่อยืนยันว่า pipeline ทำงานได้จริงทั้งแบบออนไลน์และโหมด demo
```bash
# ตัวอย่าง (ออนไลน์จริง + มี Chrome)
export CHROME_BIN=/path/to/chrome
python run_smoke_test.py --sample-size 50 --qps 4 --max-workers 8

# หากยังไม่มี Chrome: ใช้ demo สำหรับ DrugBank ชั่วคราว
# (ปรับ run_smoke_test.py ให้ส่ง --demo หรือเรียก production_drugbank_scraper.py ด้วย --demo)
```

## Quick Start
```bash
# 1) เตรียมสภาพแวดล้อม
bash scripts/setup_env.sh
source .venv/bin/activate

# 2) เตรียม vocab + openFDA data ตามโครงสร้างข้างต้น

# 3) สร้าง ER (เลือกวิธีรวดเดียวหรือทีละขั้น)
python core/openFDA_Entity_Relationship_Tables_v2.py --workers 8 -v

# 4) รัน Smoke Test (ออนไลน์จริง)
export CHROME_BIN=/path/to/chrome
python run_smoke_test.py --sample-size 50 --qps 4 --max-workers 8
```

## FAQ / Troubleshooting
- เปิด Chrome ไม่ได้เพราะ GLIBC เก่า: ใช้ Selenium Remote หรือโหมด demo ชั่วคราว
- ไม่มีเน็ตเรียก RxNav: ใช้ `RXNAV_OFFLINE=1` หรือ `--demo` ที่ `enrich/rxnav_enrich.py`
- ต้องการกรองด้วยผล DrugBank ในไฟล์สุดท้าย: รัน `enrich/production_drugbank_scraper.py` เพื่อสร้าง `drugbank_results.csv` แล้วเรียก `stage4_merge_back.py` พร้อม `--drugbank ... --filter-drugbank`

