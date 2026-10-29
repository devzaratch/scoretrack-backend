# Database Migrations

โฟลเดอร์นี้มีสคริปต์ฐานข้อมูลสำหรับ Postgres (ใช้เมื่อตั้งค่า `DATA_BACKEND=postgres` + `DATABASE_URL`)

## ไฟล์ที่มีอยู่

| ไฟล์ | คำอธิบาย |
|------|----------|
| `001_init.sql` | ตาราง `scoretrack_store` — Document store แบบ JSONB (รูปแบบเดียวกับไฟล์ JSON เดิม) |
| `002_auth_tables.sql` | ตาราง Auth V2 (user_profiles, email_to_sub, user_passwords, verification_codes, password_resets, refresh_tokens, refresh_expires, api_keys) |
| `003_chat_tables.sql` | ตารางแชท + เซสชัน (chat_reports, user_sessions) |

## วิธีใช้งาน

### ติดตั้ง Postgres แล้วรัน migration

```bash
# ติดตั้ง postgresql-client (ถ้ายังไม่มี)
# แล้วเชื่อมต่อ DB

psql "postgres://user:pass@host:5432/db" -f migrations/001_init.sql
psql "postgres://user:pass@host:5432/db" -f migrations/002_auth_tables.sql
psql "postgres://user:pass@host:5432/db" -f migrations/003_chat_tables.sql
```

### รันเซิร์ฟเวอร์ด้วย Postgres

```bash
export DATA_BACKEND=postgres
export DATABASE_URL="postgres://user:pass@host:5432/db"
go run .
```

### ตรวจสอบว่า migration รันแล้ว

```bash
psql "postgres://user:pass@host:5432/db" -c "\dt"
```

ควรเห็นตาราง: `scoretrack_store`, `user_profiles`, `email_to_sub`, `user_passwords`,
`verification_codes`, `password_resets`, `refresh_tokens`, `refresh_expires`, `api_keys`,
`chat_reports`, `user_sessions`

## หมายเหตุ

- การใช้ `fileStorage` (default) ไม่จำเป็นต้องมี migration
- Migration รันได้ซ้ำปลอดภัย (ใช้ `CREATE TABLE IF NOT EXISTS`)
- ตาราง `api_keys` เก็บเฉพาะ `key_hash` (ไม่เก็บค่า plaintext ของ API key)
