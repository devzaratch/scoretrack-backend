-- Migration 002: Auth V2 tables
-- โครงสร้าง auth: UserProfiles / EmailToSub / UserPasswords / VerificationCodes /
-- PasswordResets / RefreshTokens / RefreshExpires / APIKeys
-- ประกาศ struct ครั้งเดียวใน auth_v2.go

-- UserProfiles — โปรไฟล์ผู้ใช้ (อีเมล, ชื่อ, บทบาท, สถานะแบน, เวลาแบน)
CREATE TABLE IF NOT EXISTS user_profiles (
    sub           VARCHAR(64) PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    name          VARCHAR(255),
    role          VARCHAR(32) NOT NULL DEFAULT 'user',
    banned        BOOLEAN NOT NULL DEFAULT FALSE,
    banned_at     TIMESTAMPTZ,
    banned_reason VARCHAR(255),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_email
    ON user_profiles(email);

-- EmailToSub — แปลงอีเมลเป็น sub (สำหรับ lookup รวดเร็ว)
CREATE TABLE IF NOT EXISTS email_to_sub (
    email VARCHAR(255) PRIMARY KEY,
    sub   VARCHAR(64) NOT NULL
);

-- UserPasswords — รหัสผ่านเข้ารหัส bcrypt
CREATE TABLE IF NOT EXISTS user_passwords (
    sub        VARCHAR(64) PRIMARY KEY,
    hash       TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- VerificationCodes — รหัสยืนยันอีเมล
CREATE TABLE IF NOT EXISTS verification_codes (
    sub       VARCHAR(64) NOT NULL,
    code      VARCHAR(10) NOT NULL,
    purpose   VARCHAR(32) NOT NULL, -- verify-email | reset-password
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sub, purpose)
);

-- PasswordResets — รหัสรีเซ็ตรหัสผ่าน
CREATE TABLE IF NOT EXISTS password_resets (
    sub       VARCHAR(64) PRIMARY KEY,
    code      VARCHAR(16) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- RefreshTokens — รีเฟรชโทเค็น (1 token ต่อ email, rotate ทุกครั้งที่รีเฟรช)
CREATE TABLE IF NOT EXISTS refresh_tokens (
    sub        VARCHAR(64) PRIMARY KEY,
    token_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- RefreshExpies — เวลาหมดอายุของ refresh token (แยกต่างหากสำหรับ cleanup)
CREATE TABLE IF NOT EXISTS refresh_expires (
    sub       VARCHAR(64) PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);

-- APIKeys — API key ของผู้ใช้ (เก็บเฉพาะ hash, ไม่เก็บ plaintext)
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash  VARCHAR(64) PRIMARY KEY,
    sub       VARCHAR(64) NOT NULL,
    name      VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    revoked   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_api_keys_sub
    ON api_keys(sub);

-- END 002
