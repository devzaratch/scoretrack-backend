package main

import (
	"strings"
	"time"
)

func (db *PersistentDB) ensureAdminStateLocked() {
	if db.data.AdminAuthFails == nil {
		db.data.AdminAuthFails = make(map[string]AdminAuthFailRecord)
	}
	if db.data.AdminAuditLogs == nil {
		db.data.AdminAuditLogs = []AdminAuditRecord{}
	}
}

func pruneAdminAuthFailsLocked(m map[string]AdminAuthFailRecord, now time.Time) bool {
	changed := false
	for ip, st := range m {
		lockedUntil := time.Unix(st.LockedUntil, 0)
		windowStart := time.Unix(st.WindowStart, 0)
		if st.LockedUntil > 0 {
			if !now.Before(lockedUntil) {
				// lock expired: clear lock; keep recent fail window only if still within stale age
				if st.WindowStart == 0 || now.Sub(windowStart) > adminAuthFailStaleAge {
					delete(m, ip)
				} else {
					st.LockedUntil = 0
					st.Fails = 0
					m[ip] = st
				}
				changed = true
			}
			continue
		}
		if st.WindowStart > 0 && now.Sub(windowStart) > adminAuthFailStaleAge {
			delete(m, ip)
			changed = true
		}
	}
	return changed
}

func pruneAdminAuditLogsLocked(logs []AdminAuditRecord, now time.Time) []AdminAuditRecord {
	if len(logs) == 0 {
		return logs
	}
	cutoff := now.Add(-adminAuditRetention)
	out := make([]AdminAuditRecord, 0, len(logs))
	for _, rec := range logs {
		ts, err := time.Parse(time.RFC3339, rec.Time)
		if err == nil && ts.Before(cutoff) {
			continue
		}
		out = append(out, rec)
	}
	if len(out) > adminAuditMaxRecords {
		out = out[len(out)-adminAuditMaxRecords:]
	}
	return out
}

func (db *PersistentDB) GetAdminAuthLock(ip string, now time.Time) (locked bool, retrySec int, changed bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ensureAdminStateLocked()
	if pruneAdminAuthFailsLocked(db.data.AdminAuthFails, now) {
		db.dirty = true
		changed = true
	}
	st, ok := db.data.AdminAuthFails[ip]
	if !ok {
		return false, 0, changed
	}
	if st.LockedUntil > 0 && now.Before(time.Unix(st.LockedUntil, 0)) {
		return true, int(time.Until(time.Unix(st.LockedUntil, 0)).Seconds()) + 1, changed
	}
	if st.LockedUntil == 0 && st.WindowStart > 0 && now.Sub(time.Unix(st.WindowStart, 0)) > 10*time.Minute {
		delete(db.data.AdminAuthFails, ip)
		db.dirty = true
		return false, 0, true
	}
	return false, 0, changed
}

func (db *PersistentDB) NoteAdminAuthFail(ip string, now time.Time, maxFails, lockMinutes int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ensureAdminStateLocked()
	st := db.data.AdminAuthFails[ip]
	windowStart := time.Unix(st.WindowStart, 0)
	if st.WindowStart == 0 || (st.LockedUntil == 0 && now.Sub(windowStart) > 10*time.Minute) {
		st = AdminAuthFailRecord{WindowStart: now.Unix()}
	}
	st.Fails++
	if st.Fails >= maxFails {
		st.LockedUntil = now.Add(time.Duration(lockMinutes) * time.Minute).Unix()
	}
	db.data.AdminAuthFails[ip] = st
	if pruneAdminAuthFailsLocked(db.data.AdminAuthFails, now) {
		// already changed
	}
	db.dirty = true
}

func (db *PersistentDB) ResetAdminAuthFails(ip string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ensureAdminStateLocked()
	if _, ok := db.data.AdminAuthFails[ip]; ok {
		delete(db.data.AdminAuthFails, ip)
		db.dirty = true
	}
}

func (db *PersistentDB) AppendAdminAudit(rec AdminAuditRecord) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ensureAdminStateLocked()
	now := time.Now()
	if strings.TrimSpace(rec.Time) == "" {
		rec.Time = now.UTC().Format(time.RFC3339)
	}
	logs := append(db.data.AdminAuditLogs, rec)
	db.data.AdminAuditLogs = pruneAdminAuditLogsLocked(logs, now)
	db.dirty = true
}

func (db *PersistentDB) AdminAuditLogs(limit int) []AdminAuditRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	logs := db.data.AdminAuditLogs
	if len(logs) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(logs) {
		limit = len(logs)
	}
	out := make([]AdminAuditRecord, limit)
	copy(out, logs[len(logs)-limit:])
	// newest first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func (db *PersistentDB) PruneAdminAuthState(now time.Time) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.ensureAdminStateLocked()
	changed := pruneAdminAuthFailsLocked(db.data.AdminAuthFails, now)
	prunedLogs := pruneAdminAuditLogsLocked(db.data.AdminAuditLogs, now)
	if len(prunedLogs) != len(db.data.AdminAuditLogs) {
		db.data.AdminAuditLogs = prunedLogs
		changed = true
	}
	if changed {
		db.dirty = true
	}
}
