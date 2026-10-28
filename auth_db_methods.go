package main

// --- PersistentDB methods for Auth V2 ---
// (structs UserProfile / VerificationCode / PasswordReset อยู่ใน auth_v2.go)

// SaveUserProfile บันทึก profile ของผู้ใช้
func (db *PersistentDB) SaveUserProfile(sub string, prof UserProfile, passwordHash string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.UserProfiles[sub] = prof
	db.data.EmailToSub[prof.Email] = sub
	db.data.UserPasswords[prof.Email] = passwordHash
	db.dirty = true
}

// GetUserProfile ดึง profile จาก sub
func (db *PersistentDB) GetUserProfile(sub string) (UserProfile, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	prof, ok := db.data.UserProfiles[sub]
	return prof, ok
}

// GetUserProfileByEmail ค้นหา profile จากอีเมล (สำหรับ login/register ตรวจซ้ำ)
func (db *PersistentDB) GetUserProfileByEmail(email string) *UserProfile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, prof := range db.data.UserProfiles {
		if prof.Email == email {
			return &prof
		}
	}
	return nil
}

// MarkUserVerified ตั้งค่า verified = true
func (db *PersistentDB) MarkUserVerified(sub string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if prof, ok := db.data.UserProfiles[sub]; ok {
		prof.Verified = true
		db.data.UserProfiles[sub] = prof
		db.dirty = true
	}
}

// DeleteVerificationCode ลบรหัสยืนยันเก่า
func (db *PersistentDB) DeleteVerificationCode(email string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data.VerificationCodes, email)
	db.dirty = true
}

// SaveVerificationCode บันทึกรหัสยืนยันอีเมล
func (db *PersistentDB) SaveVerificationCode(email string, vc VerificationCode) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.VerificationCodes[email] = vc
	db.dirty = true
}

// GetVerificationCode ดึงข้อมูล Rhode ยืนยัน
func (db *PersistentDB) GetVerificationCode(email string) (VerificationCode, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	vc, ok := db.data.VerificationCodes[email]
	return vc, ok
}

// GetPasswordHashByEmail ดึง hash password จากอีเมล (สำหรับ login)
func (db *PersistentDB) GetPasswordHashByEmail(email string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data.UserPasswords[email]
}

// SavePasswordReset บันทึกรหัสรีเซ็ตรหัสผ่าน
func (db *PersistentDB) SavePasswordReset(email string, pr PasswordReset) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.PasswordResets[email] = pr
	db.dirty = true
}

// GetPasswordReset ดึงข้อมูลรหัสรีเซ็ต (สำหรับยืนยัน)
func (db *PersistentDB) GetPasswordReset(email string) (PasswordReset, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	pr, ok := db.data.PasswordResets[email]
	return pr, ok
}

// UpdatePasswordHash อัปเดตรหัสผ่านใหม่
func (db *PersistentDB) UpdatePasswordHash(email string, hashStr string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.UserPasswords[email] = hashStr
	db.dirty = true
}

// DeletePasswordReset ลบรหัสรีเซ็ตหลังใช้งานแล้ว
func (db *PersistentDB) DeletePasswordReset(email string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.data.PasswordResets, email)
	db.dirty = true
}

// GetSubByRefreshToken ค้นหา sub จาก refresh token
func (db *PersistentDB) GetSubByRefreshToken(token string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for sub, rt := range db.data.RefreshTokens {
		if rt == token {
			return sub
		}
	}
	return ""
}

// SaveRefreshToken บันทึก refresh token ของ sub ไว้
func (db *PersistentDB) SaveRefreshToken(sub string, token string, expiresAt int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.RefreshTokens[sub] = token
	db.data.RefreshExpires[sub] = expiresAt
	db.dirty = true
}

// DeleteRefreshToken ลบ refresh token เมื่อ logout หรือ rotate
func (db *PersistentDB) DeleteRefreshToken(token string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for sub, rt := range db.data.RefreshTokens {
		if rt == token {
			delete(db.data.RefreshTokens, sub)
			delete(db.data.RefreshExpires, sub)
			db.dirty = true
			return
		}
	}
}

// EnsureAuthDefaults — เรียกที่ InitDB เพื่อสร้าง map เปล่าทั้งหมด (ไม่จำเป็นต้องเรียกซ้ำเพราะทำใน InitDB แล้ว)
func (db *PersistentDB) EnsureAuthDefaults() {
	// map ถูกสร้างใน InitDB แล้ว แต่ยืนยันไม่เป็น nil
	if db.data.UserProfiles == nil {
		db.data.UserProfiles = make(map[string]UserProfile)
	}
	if db.data.EmailToSub == nil {
		db.data.EmailToSub = make(map[string]string)
	}
	if db.data.UserPasswords == nil {
		db.data.UserPasswords = make(map[string]string)
	}
	if db.data.VerificationCodes == nil {
		db.data.VerificationCodes = make(map[string]VerificationCode)
	}
	if db.data.PasswordResets == nil {
		db.data.PasswordResets = make(map[string]PasswordReset)
	}
	if db.data.RefreshTokens == nil {
		db.data.RefreshTokens = make(map[string]string)
	}
	if db.data.RefreshExpires == nil {
		db.data.RefreshExpires = make(map[string]int64)
	}
}

// --- Phase A ข้อ 3: จัดการอุปกรณ์/เซสชัน ---

// CountSessionsBySub นับจำนวน session token ที่ผูกกับ sub นี้ (มีเครื่องกี่เครื่อง)
func (db *PersistentDB) CountSessionsBySub(sub string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	n := 0
	for _, s := range db.data.UserSessions {
		if s == sub {
			n++
		}
	}
	return n
}

// GetRefreshExpiryBySub ดึงเวลาหมดอายุ refresh token ของ sub (โมเดลเดียวต่อ 1 โหมด)
func (db *PersistentDB) GetRefreshExpiryBySub(sub string) (int64, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if _, ok := db.data.RefreshTokens[sub]; !ok {
		return 0, false
	}
	return db.data.RefreshExpires[sub], true
}

// DeleteRefreshTokenBySub ลบ refresh token ของ sub ทั้งหมด (ออกจากระบบทุกเครื่อง)
func (db *PersistentDB) DeleteRefreshTokenBySub(sub string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.data.RefreshTokens[sub]; ok {
		delete(db.data.RefreshTokens, sub)
		delete(db.data.RefreshExpires, sub)
		db.dirty = true
	}
}

// --- Phase B ข้อ 4: จัดการบัญชีจากหน้า admin ---

// ListUserProfiles รายการบัญชี (email/Google) ทั้งหมด — สำหรับหน้า admin
func (db *PersistentDB) ListUserProfiles() []UserProfile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]UserProfile, 0, len(db.data.UserProfiles))
	for _, prof := range db.data.UserProfiles {
		out = append(out, prof)
	}
	return out
}

// SetUserBanned ตั้ง/ยกเลิกการระงับบัญชี คืน false ถ้าไม่พบ sub
func (db *PersistentDB) SetUserBanned(sub string, banned bool) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	prof, ok := db.data.UserProfiles[sub]
	if !ok {
		return false
	}
	prof.Banned = banned
	db.data.UserProfiles[sub] = prof
	db.dirty = true
	return true
}

// SetUserRole เปลี่ยน role บัญชี ("user"|"admin") คืน false ถ้าไม่พบ sub หรือ role ไม่ถูกต้อง
func (db *PersistentDB) SetUserRole(sub, role string) bool {
	if role != "user" && role != "admin" {
		return false
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	prof, ok := db.data.UserProfiles[sub]
	if !ok {
		return false
	}
	prof.Role = role
	db.data.UserProfiles[sub] = prof
	db.dirty = true
	return true
}

// --- Phase B ข้อ 10: รายงานข้อความแชท ---

// ChatReport รายงานข้อความที่ไร้ความเหมาะสม (ผู้ใช้กดรายงาน-ข้อความของคนอื่นให้แอดมินดู)
type ChatReport struct {
	MatchID    string `json:"match_id"`
	User       string `json:"user"` // ชื่อที่แสดงในแชท
	Text       string `json:"text"` // ข้อความ (ตัดสั้น)
	Reason     string `json:"reason"`
	ReporterIP string `json:"reporter_ip"`
	ReportedAt int64  `json:"reported_at"`
}

// AddChatReport เพิ่มรายงาน (คืน true ถ้าสำเร็จ)
func (db *PersistentDB) AddChatReport(rep ChatReport) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data.ChatReports = append(db.data.ChatReports, rep)
	// จำกัดจำนวนสะสม (กันไฟล์บวม) — เก็บไม่เกิน 1,000 รายล่าสุด
	if len(db.data.ChatReports) > 1000 {
		db.data.ChatReports = db.data.ChatReports[len(db.data.ChatReports)-1000:]
	}
	db.dirty = true
	return true
}

// ListChatReports รายการรายงาน (ใหม่สุดก่อน)
func (db *PersistentDB) ListChatReports() []ChatReport {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]ChatReport, 0, len(db.data.ChatReports))
	for i := len(db.data.ChatReports) - 1; i >= 0; i-- {
		out = append(out, db.data.ChatReports[i])
	}
	return out
}

// RemoveChatReport ลบรายงานตาม index ของรายการใน DB (แอดมินปิดรายงาน = ลบ) คืน false ถ้า index เกิน
func (db *PersistentDB) RemoveChatReport(idx int) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	if idx < 0 || idx >= len(db.data.ChatReports) {
		return false
	}
	db.data.ChatReports = append(db.data.ChatReports[:idx], db.data.ChatReports[idx+1:]...)
	db.dirty = true
	return true
}