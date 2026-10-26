// k6 load test — Score-track backend (แผนใหม่ข้อ 7)
//
// วิธีรัน (ติดตั้ง k6 ก่อน: https://k6.io/docs/get-started/installation/)
//   k6 run -e BASE_URL=http://localhost:8080 go-backend/loadtest/k6-live.js
//   k6 run -e BASE_URL=https://scoretrack-backend.onrender.com go-backend/loadtest/k6-live.js
//
// เป้าหมาย: รู้ขีดจำกัดจริงก่อนแมตช์ใหญ่ — ไม่ใช่เดาจากจำนวน instance
// ปรับ VU/ระยะเวลาใน options ตาม plan ของ Render ที่ใช้จริง

import http from "k6/http";
import ws from "k6/ws";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const WS_URL = __ENV.WS_URL || BASE.replace(/^http/, "ws") + "/ws/live";

// metric เสริม: จำนวน WS ที่ต่อไม่สำเร็จ (แยกจาก HTTP error)
const wsFailRate = new Rate("ws_connect_fail");

export const options = {
  scenarios: {
    // 1) HTTP อ่านข้อมูล — จำลองคนเปิดหน้าเว็บ/กดรีเฟรช
    http_reads: {
      executor: "ramping-vus",
      startVUs: 5,
      stages: [
        { duration: "30s", target: 50 }, // ค่อยๆ ไต่
        { duration: "60s", target: 50 }, // นิ่งที่ 50 VU
        { duration: "30s", target: 0 },
      ],
      exec: "httpReads",
    },
    // 2) WebSocket ค้างฟัง — จำลองคนเปิดหน้า live ค้างไว้
    ws_listeners: {
      executor: "ramping-vus",
      startVUs: 5,
      stages: [
        { duration: "30s", target: 100 },
        { duration: "90s", target: 100 },
        { duration: "30s", target: 0 },
      ],
      exec: "wsListen",
      // WS ใช้ connection ยาว — VU น้อยแต่ถือ socket ได้นาน (ตั้งตาม plan จริง)
      gracefulStop: "10s",
    },
  },
  thresholds: {
    // SLO เบื้องต้น — ปรับให้ตรงกับ SLO จริงของเรา (หน้า admin /api/admin/metrics)
    http_req_failed: ["rate<0.01"], // error < 1%
    http_req_duration: ["p(95)<800", "p(99)<2000"],
    ws_connect_fail: ["rate<0.02"], // ต่อ WS ไม่ติด < 2%
  },
};

// endpoint อ่านที่คนใช้บ่อยจริง (GET ล้วน — ไม่แตะ auth/prediction)
const READ_PATHS = ["/api/matches", "/api/standings", "/api/news"];

export function httpReads() {
  for (const path of READ_PATHS) {
    const res = http.get(`${BASE}${path}`, {
      tags: { path },
      timeout: "15s",
    });
    check(res, {
      "status 2xx/3xx": (r) => r.status >= 200 && r.status < 400,
    });
    sleep(Math.random() * 2 + 1);
  }
  // จำลองการเปลี่ยนหน้า: หยุดสั้นๆ แล้ววน
  sleep(Math.random() * 3 + 2);
}

export function wsListen() {
  const res = ws.connect(WS_URL, { tags: { scenario: "ws_live" } }, (socket) => {
    socket.on("open", () => {
      wsFailRate.add(0);
    });
    socket.on("error", () => {
      wsFailRate.add(1);
    });
    // ถือ connection ไว้ ~20 วิ (เหมือนผู้ใช้เปิดหน้า live ค้าง)
    socket.setTimeout(() => socket.close(), 20000);
  });

  const ok = check(res, { "ws handshake 101": (r) => r && r.status === 101 });
  if (!ok) wsFailRate.add(1);
  sleep(1);
}
