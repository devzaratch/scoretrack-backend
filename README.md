# ScoreTrack Go Backend

เซิร์ฟเวอร์หลักของโปรเจค ScoreTrack (แอปผลบอลสด/ลivescores) เขียนด้วยภาษา Go + Gin Framework ใช้งาน Redis สำหรับแคชและการแจ้งเตือนแบบเรียลไทม์ผ่าน WebSocket

## โครงสร้างไฟล์หลัก

| ไฟล์ | คำอธิบาย |
|------|----------|
| `main.go` | จุดเริ่มต้นเซิร์ฟเวอร์ (router, routes, WebSocket hub, middleware) |
| `auth_v2.go` | ระบบ Auth V2 — ลงทะเบียน/เข้าสู่ระบบ/รีเฟรชโทเค็น/ล็เอาต์/แบนผู้ใช้ |
| `user_selfserve.go` | ระบบบริการตัวเอง — API keys, รายงานแชท |
| `redcard_detector.go` | ตรวจจับใบแดงแบบเรียลไทม์จากข้อมูลการแข่งขัน + ส่งแจ้งเตือน (FCM + WS) |
| `provider_goalapi.go` / `provider_apifootball.go` | การดึงข้อมูลแมตช์จากผู้ให้บริการภายนอก |
| `auth_db_methods.go` | ฟังก์ชันฐานข้อมูลสำหรับ Auth (ข้อมูลผู้ใช้, เซสชัน, รายงานแชท, รหัสผ่าน) |
| `db_backup.go` | สำรองข้อมูลฐานข้อมูล + เครื่องมือตรวจสอบ |
| `image_cache.go` | ระบบแคชรูปภาพ proxy |
| `chat_polls.go` | ระบบโหวตในแชท |

## การติดตั้งและรัน

### 1. Clone และติดตั้ง Go module

```bash
cd go-backend
go mod tidy
```

### 2. ตั้งค่า Environment Variables

สร้างไฟล์ `.env` ในโฟลเดอร์ `go-backend`:

```env
# เซิร์ฟเวอร์
PORT=8080
GIN_MODE=release

# Redis
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

# อีเมล SMTP (ถ้าไม่กำหนดจะใช้ logging แทนการส่งจริง)
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASS=
SMTP_FROM=

# Auth V2
JWT_SECRET=your-secret-change-me
ADMIN_TOKEN=your-admin-token-change-me

# ผู้ให้บริการข้อมูลแมตช์ (เลือกได้หลายตัว)
GOAL_API_KEY=
APIFOOTBALL_API_KEY=
```

### 3. รันเซิร์ฟเวอร์

```bash
go run .
```

เซิร์ฟเวอร์จะรันที่ `http://localhost:8080`

### 4. ทดสอบ health check

```bash
curl http://localhost:8080/health
```

## Endpoints หลัก

### Auth
| Method | Path | คำอธิบาย |
|--------|------|----------|
| POST | `/auth/register` | ลงทะเบียนผู้ใช้ใหม่ |
| POST | `/auth/login` | เข้าสู่ระบบ (ได้คุกกี้ session) |
| POST | `/auth/session` | ดึงข้อมูลเซสชันปัจจุบัน |
| POST | `/auth/logout` | ออกจากระบบ |
| POST | `/auth/refresh` | รีเฟรชโทเค็นเข้าสู่ระบบ |
| POST | `/auth/verify-email` | ยืนยันอีเมล |
| POST | `/auth/resend-verify` | ส่งรหัสยืนยันอีเมลซ้ำ |

### ผู้ใช้ (ต้อง login)
| Method | Path | คำอธิบาย |
|--------|------|----------|
| GET | `/user/profile` | ดูโปรไฟล์ผู้ใช้ |
| POST | `/user/favorites` | บันทึกทีมที่ชอบ |
| POST | `/user/fcm` | ลงทะเบียน token สำหรับแจ้งเตือน push |
| POST | `/user/match-subscribe` | ติดตามแมตช์ที่สนใจ |
| POST | `/user/notifications` | ตั้งค่าการแจ้งเตือน |

### API Keys (บริการตัวเอง)
| Method | Path | คำอธิบาย |
|--------|------|----------|
| GET | `/auth/api-keys` | ดู API keys ของตัวเอง |
| POST | `/auth/api-keys` | สร้าง API key ใหม่ |
| DELETE | `/auth/api-keys/:id` | ยกเลิก API key |

### แชท (ต้อง login)
| Method | Path | คำอธิบาย |
|--------|------|----------|
| POST | `/chat/report` | รายงานข้อความที่ไม่เหมาะสม |

### ระบบ
| Method | Path | คำอธิบาย |
|--------|------|----------|
| GET | `/health` | ตรวจสอบสถานะเซิร์ฟเวอร์ |

## ระบบทดสอบอัตโนมัติ

```bash
# รันทดสอบทั้งหมด
go test ./...

# ตรวจสอบโค้ดด้วย vet
go vet ./...

# สร้างไฟล์ binary
go build .
```

## เทคโนโลยีที่ใช้

- **Go** — ภาษาหลักของเซิร์ฟเวอร์
- **Gin** — Web framework สำหรับ HTTP routes
- **Redis** — แคช + ตัวกลางส่งข้อความเรียลไทม์
- **WebSocket** — การสื่อสารแบบสองทางสำหรับคะแนนสดและใบแดง
- **bcrypt** — เข้ารหัสรหัสผ่านผู้ใช้

## หมายเหตุสำหรับนักพัฒนา

- Auth V2 เก็บข้อมูลใน `StoreData` รูปแบบ JSONB (โปรไฟล์ผู้ใช้, รหัสผ่าน, รหัสยืนยัน, รีเฟรชโทเค็น)
- ระบบแบนผู้ใช้จะตรวจสอบทุกครั้งที่เข้าสู่ระบบและรีเฟรชโทเค็น
- คุกกี้ session ใช้ `HttpOnly` และ `SameSite=Lax` เพื่อความปลอดภัย
- ระบบอัปเดตคะแนนและใบแดงแบบเรียลไทม์ผ่าน WebSocket channel
- การส่งอีเมลจะทำงานก็ต่อเมื่อมีการตั้งค่า SMTP env vars เท่านั้น มิฉะนั้นจะ log ข้อความแทน
