# Load & Capacity Test (k6)

ทดสอบขีดจำกัดจริงของ backend **ก่อนแมตช์ใหญ่** — ไม่เดาจาก spec ของ Render

## ติดตั้ง k6

```powershell
winget install k6
# หรือ: choco install k6
```

## รัน

```powershell
# local (ต้อง start backend ก่อน: go run .)
k6 run -e BASE_URL=http://localhost:8080 loadtest/k6-live.js

# production (ระวัง! ยิงจริง — ใช้ตอน off-peak และแจ้งทีมก่อน)
k6 run -e BASE_URL=https://scoretrack-backend.onrender.com loadtest/k6-live.js

# เร็วๆ เช็คเฉพาะ HTTP (ไม่เปิด WS 100 ตัว)
k6 run -e BASE_URL=http://localhost:8080 --vus 10 --duration 30s loadtest/k6-live.js
```

## วิธีอ่านผล

| metric | ความหมาย | เป้าหมาย |
|---|---|---|
| `http_req_duration p(95)` | เวลาตอบ 95% ของ request | < 800ms |
| `http_req_failed` | request ล้มเหลว | < 1% |
| `ws_connect_fail` | ต่อ WebSocket ไม่ติด | < 2% |
| `vus_max` | จำนวน VU สูงสุดที่รันได้ | ตาม plan |

## ขั้นตอนแนะนำ

1. รัน local ที่ 50 VU HTTP + 100 WS → เก็บ baseline
2. รัน prod off-peak (เช่น ตี 3) → เทียบกัน
3. ไต่ขึ้นเรื่อยๆ (100 → 250 → 500 WS) จนเจอจุดพัง
4. จดตัวเลขที่เริ่มพัง แล้วปรับ Render plan / เพิ่ม Redis PubSub (ถ้าถึงขีด)

> หมายเหตุ: `GlobalRateLimiter` จำกัด 120 req/min ต่อ IP — รัน load test จากเครื่องเดียว
> จะชน limit นี้ก่อน (ได้ 429) ถ้าต้องการทดสอบ throughput จริงให้ใช้ `rateLimitScale`
> env หรือยิงจากหลายเครื่อง/หลาย IP
