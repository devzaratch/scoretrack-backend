// Backup verification — ตรวจสอบว่า backup มีครบ ล้างแฟ้มที่ไม่ใช่ล่าสุด
import { readdirSync, statSync, unlinkSync, existsSync } from "node:fs";
import { join } from "node:path";

const BACKUP_DIR = process.argv[2] || "./data/backups";

function verify() {
  try {
    const dir = dirname(BACKUP_DIR);
    if (!existsSync(dir)) {
      console.error("Backup dir ไม่มีอยู่:", dir);
      process.exit(1);
    }
    const files = readdirSync(dir).filter((f) => f.endsWith(".json"));
    const totalSize = files.reduce((sum, f) => sum + statSync(join(dir, f)).size, 0);
    console.log("=== Backup Verification ===");
    console.log("Dir:", dir);
    console.log(`Files: ${files.length}, Total size: ${totalSize} bytes`);

    // ตรวจสอบทุกไฟล์: ล้างที่อายุ >24 ชม. (เก็บไว้แค่ล่าสุด)
    const now = Date.now();
    for (const f of files) {
      const ageMs = now - statSync(join(dir, f)).mtimeMs;
      const ageMin = ageMs / 60000;
      if (ageMin > 1440) {
        unlinkSync(join(dir, f));
        console.log(`⚠️  Firedel (over 24h): ${f}`);
      } else {
        console.log(`✅ Fresh: ${f} (${ageMin.toFixed(0)} min)`);
      }
    }
  } catch (e) {
    console.error("Verify failed:", e);
    process.exit(1);
  }
}

verify();
