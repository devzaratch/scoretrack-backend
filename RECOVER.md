# RECOVER.md — คู่มือซ้อมกู้ข้อมูลผู้ใช้ (ข้อ 9: restore rehearsal)

> เอกสารสำหรับผู้ดูแลระบบ — อ่านจบ 5 นาที ทำได้จริง 15 นาที
> วัตถุประสงค์: พิสูจน์ว่าเกิดดิสก์หายจริงบน Render แล้วข้อมูลผู้ใช้ (บัญชี, คู่โปรด, การทำนาย, สิทธิ์ API key, FCM tokens) กู้กลับมาครบ

---

## ระบบกู้คืนทำงานอย่างไร

1. ทุกครั้งที่ flush สำเร็จ ระบบ sync snapshot ทั้ง `store.json` ขึ้น Redis
   (key: `scoretrack:db:snapshot`, throttle 60 วินาที/ครั้ง)
2. ตอน boot ถ้าไฟล์ local ว่าง (Render ล้างดิสก์) → `restoreFromSnapshot()` ดึงจาก Redis อัตโนมัติ
3. มี drill อัตโนมัติวิ่งตลอด: `startBackupDrillLoop()` + CI step `go test -run TestBackupDrill`

## ต้องมีอะไรก่อนซ้อม

- `REDIS_URL` ตั้งถูกต้อง (Upstash) — ยืนยันด้วย log "redis snapshot saved" เมื่อ flush หลังบันทึกข้อมูล
- snapshot มีข้อมูลจริง: จำนวน record > 0 (ดู log drill บอกจำนวน sessions/favorites/predictions/tokens/subs)

## ขั้นตอนซ้อมกู้ (manual rehearsal)

1. **บันทึก baseline** ก่อน: log drill ปัจจุบัน (จำนวน user records) หรือเรียก `GET /healthz` + snapshot log
2. เลือกสภาพแวดล้อมทดสอบ (ทำบน **environment แยก** หรือหยุด service ชั่วคราว):
   - วิธีจำลองดิสก์หาย: ลบ/ย้ายไฟล์ `data/store.json` ออก แล้วรีสตาร์ท process ใหม่
3. ตอน boot ใหม่ — ใน log ต้องเห็น "restore snapshot จาก Redis" พร้อมจำนวน record
4. **ยืนยันความครบถ้วน** (ทุกข้อต้องผ่าน):
   - `GET /healthz` → status `healthy`
   - จำนวน records ใน log = baseline ก่อนซ้อม (sessions/favorites/predictions/FCM/subs)
   - ลองเข้าเว็บกับ account จริง 1 ตัว: คู่โปรด + การทำนายยังติดตาม
5. ซ้อมซ้ำ 1 ครั้งเมื่อเปลี่ยน env หรือ upgrade Redis — ถ่ายภาพ log แนบรายงาน

## ข้อจำกัดที่ต้องรับให้ได้

- Chat messages ไม่นับเป็น "user records" — ข้อความแชทอาจถูกกรองตาม retention 30 วัน (จงใจ)
- Quota ฟรี Upstash จำกัด commands — flush ถูก throttle 60 วิ/ครั้ง ตามดีไซน์
- ข้อมูลที่ยังไม่เคย flush สำเร็จ (ซ้ำความผิดพลาดใน 1 นาทีล่าสุด) = สูญหายได้ (RPO ≤ 1 นาที)

## สถานะการซ้อม (บันทึกทุกรอบ)

| วันที่ | Environment | Baseline | กู้คืนครบ | ผู้เล่น 1 account ยืนยัน | ผู้บันทึก |
|---|---|---|---|---|---|
| _(ยังไม่ได้ซ้อม — ทำรอบแรกตามขั้นตอนด้านบน)_ | | | | | |
