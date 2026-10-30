-- Migration 003: Chat & moderation tables
-- รายงานข้อความแชท + เซสชัน + เซสชันล็อกอิน (สำหรับหน้า admin + โปรไฟล์)

-- ChatReports — รายงานข้อความแชทที่ไม่เหมาะสม
CREATE TABLE IF NOT EXISTS chat_reports (
    id          SERIAL PRIMARY KEY,
    reporter_id VARCHAR(64) NOT NULL,
    target_id   VARCHAR(64),
    message_id  VARCHAR(64),
    reason      TEXT NOT NULL,
    status      VARCHAR(32) NOT NULL DEFAULT 'pending', -- pending | reviewed | dismissed | actioned
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    reviewed_by VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_chat_reports_status
    ON chat_reports(status);

CREATE INDEX IF NOT EXISTS idx_chat_reports_created_at
    ON chat_reports(created_at);

-- Chat messages (history for moderation / replay)
CREATE TABLE IF NOT EXISTS chat_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    match_id VARCHAR(64),
    user_sub VARCHAR(64) NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_match_id
    ON chat_messages(match_id);

CREATE INDEX IF NOT EXISTS idx_chat_messages_user_sub
    ON chat_messages(user_sub);

-- Sessions — セッションข้อมูลการล็อกอิน (สำหรับ SessionsCard ในโปรไฟล์)
CREATE TABLE IF NOT EXISTS user_sessions (
    id        VARCHAR(64) PRIMARY KEY,
    sub       VARCHAR(64) NOT NULL,
    ip        VARCHAR(64),
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_sub
    ON user_sessions(sub);

-- END 003
