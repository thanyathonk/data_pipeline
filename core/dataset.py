import os
from huggingface_hub import snapshot_download


REPO_ID = "tonnnnnnnnnnnnn/FDA_patient"  # เปลี่ยนได้ตามจริงของคุณใน HF Hub
TARGET_FOLDER = "openFDA_drug_event"      # โฟลเดอร์ที่ต้องการดาวน์โหลดจากรีโป
OUT_DIR = "data"                          # โฟลเดอร์ปลายทางในโปรเจกต์


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Downloading '{TARGET_FOLDER}' from '{REPO_ID}' to '{OUT_DIR}' ...")

    # ดาวน์โหลดเฉพาะโฟลเดอร์ที่กำหนดลง OUT_DIR โดยไม่ใช้ symlink
    snapshot_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        allow_patterns=[f"{TARGET_FOLDER}/**"],
        local_dir=OUT_DIR,
        local_dir_use_symlinks=False,
    )

    print(f"Done. Files are in '{os.path.join(OUT_DIR, TARGET_FOLDER)}'")


if __name__ == "__main__":
    main()




