-- Migration 001: StoreData JSONB document store (postgresStorage)
-- รองรับ DATA_BACKEND=postgres
-- ตารางนี้เก็บข้อมูลทั้งหมดเป็น JSONB document เดียว (รูปแบบเดียวกับไฟล์ data/store.json เดิม)
-- ต่อยอดเป็นตารางจริงทีละตารางได้โดยไม่ต้องแตะ business logic

CREATE TABLE IF NOT EXISTS scoretrack_store (
    key   VARCHAR(255) PRIMARY KEY,
    data  JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scoretrack_store_updated_at
    ON scoretrack_store(updated_at);

-- END 001
