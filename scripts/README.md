# Scripts (เครื่องมือเสริม)

ในโฟลเดอร์ `scripts/` มีเครื่องมือเสริมสำหรับช่วยเตรียมสภาพแวดล้อมและจัดการข้อมูล เช่น อัปโหลด/ดาวน์โหลดข้อมูลกับ Google Drive และติดตั้ง dependencies ตาม `requirements.txt`

## 1) อัปโหลดไปยัง Google Drive — `upload_to_gdrive.py`
สคริปต์: `scripts/upload_to_gdrive.py`

- หน้าที่: อัปโหลดไฟล์หรือโฟลเดอร์ (รองรับอัปโหลดแบบ recursive โดยตรง หรือ zip เป็นไฟล์เดียวก่อนอัปโหลด)
- การยืนยันสิทธิ์ (OAuth):
  - ครั้งแรกต้องเตรียมไฟล์ OAuth client JSON (เช่น `credentials/credentials.json`) แล้วทำตามขั้นตอนที่สคริปต์แนะนำ (เปิดลิงก์/คัดลอกโค้ดยืนยัน)
  - โทเคนจะถูกเก็บไว้ใน `credentials/token.json` เพื่อใช้ครั้งต่อไปโดยไม่ต้องยืนยันใหม่
- พารามิเตอร์ที่ใช้บ่อย:
  - `--src`: ไฟล์หรือโฟลเดอร์ต้นทาง
  - `--folder-id`: ID ของโฟลเดอร์ปลายทางบน Google Drive (ดูจาก URL โฟลเดอร์)
  - `--zip`: บีบอัดโฟลเดอร์เป็น zip ก่อนอัปโหลด (ตัวเลือก)
  - `--name`: ชื่อไฟล์/โฟลเดอร์ปลายทาง (ไม่ระบุจะใช้ชื่อเดิม)
  - `--credentials`: path ไปยังไฟล์ client JSON (ดีฟอลต์: `credentials/credentials.json`)
  - `--token`: path ของ token (ดีฟอลต์: `credentials/token.json`)

ตัวอย่างการใช้งาน:

- อัปโหลดโฟลเดอร์แบบ recursive (ไม่ zip) และแสดง progress ต่อไฟล์
```bash
python scripts/upload_to_gdrive.py \
  --src data/openFDA_drug_event \
  --folder-id <GDRIVE_FOLDER_ID> \
  --credentials credentials/credentials.json
```

- ใช้ไฟล์ตั้งค่า (ไม่ต้องพิมพ์พารามิเตอร์ยาวๆ)
  - สร้างไฟล์ `scripts/gdrive_upload.json` เช่น:
```json
{
  "src": "data/openFDA_drug_event",
  "folder_id": "<GDRIVE_FOLDER_ID>",
  "zip": true,
  "name": "openFDA_drug_event_20250101_000000.zip",
  "auth_mode": "service",
  "service_account": "credentials/service_account.json",
  "credentials": "credentials/credentials.json",
  "token": "credentials/token.json"
}
```
  - รันสั้นๆ ด้วย:
```bash
python scripts/upload_to_gdrive.py --config scripts/gdrive_upload.json
```

หมายเหตุ:
- CLI ที่ส่งเข้ามาจะ override ค่าในไฟล์ config (ถ้าต้องการเปลี่ยนเฉพาะบางอัน)
- หากต้องการควบคุมที่วางไฟล์ zip ให้ใช้ `zip_out`, เช่น `"zip_out": "/md0/tmp/openFDA.zip"`

- อัปโหลดโฟลเดอร์แบบ zip ไฟล์เดียว (ต้องมีพื้นที่ว่างเพียงพอ)
```bash
python scripts/upload_to_gdrive.py \
  --src data/openFDA_drug_event \
  --zip \
  --name openFDA_drug_event_$(date +%Y%m%d_%H%M%S).zip \
  --folder-id <GDRIVE_FOLDER_ID> \
  --credentials credentials/credentials.json
```

หมายเหตุ:
- สคริปต์พยายามหลีกเลี่ยงการสร้างโฟลเดอร์ซ้ำ (duplicate) โดยจะตรวจและ reuse โฟลเดอร์เดิมภายใต้ parent เดียวกันอัตโนมัติ
- โฟลเดอร์ `credentials/` ถูกกันไว้ใน `.gitignore` ไม่ควร commit ขึ้น Git

## 2) ดาวน์โหลดจาก Google Drive/HTTP — `fetch_openfda_data.py`
สคริปต์: `scripts/fetch_openfda_data.py`

- หน้าที่: ดาวน์โหลดไฟล์ archive (zip/tar.*) ที่บรรจุ `openFDA_drug_event/` และ/หรือ `er_tables/` แล้วแตกลง `data/openFDA_drug_event/` เพื่อพร้อมใช้งานก่อนรัน pipeline
- รองรับดาวน์โหลดจาก Google Drive (ผ่าน gdown) และ HTTP(S) พร้อมแถบ progress
- ตรวจสอบ SHA256 ได้ (ตัวเลือก)

ตัวอย่างการใช้งาน:
```bash
# ดาวน์โหลดและแตกไฟล์ลง data/openFDA_drug_event
python scripts/fetch_openfda_data.py \
  --url "https://drive.google.com/file/d/<FILE_ID>/view?usp=sharing" \
  --out data/openFDA_drug_event \
  --sha256 <OPTIONAL_SHA256>
```

หมายเหตุ:
- หากใช้ลิงก์ Google Drive ควรเปิดแชร์ไฟล์ (Anyone with the link) หรือใช้ลิงก์ที่รองรับ gdown
- เมื่อแตกไฟล์แล้ว หากใน `data/openFDA_drug_event/` มี `er_tables/` ครบ สามารถข้าม Stage‑1/2 และรัน Stage‑3 → 10 ได้ทันที

## 3) เตรียมสภาพแวดล้อม
```bash
# สร้าง virtualenv และติดตั้ง dependencies
bash scripts/setup_env.sh
source .venv/bin/activate
```

## 4) ข้อควรระวัง
- อย่า commit ไฟล์ลับใน `credentials/` ขึ้น Git
- โฟลเดอร์ข้อมูล/ผลลัพธ์ขนาดใหญ่ถูกกันไว้ใน `.gitignore` แล้ว (เช่น `data/`, `data/test_runs/`, `data/release/`)
