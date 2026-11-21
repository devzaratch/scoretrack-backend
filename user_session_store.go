package main

import (
	"bytes"
	"encoding/json"
)

// UserSession is the server-side session record (token -> sub + lifecycle metadata).
// Legacy persisted format stored only the subject string; custom unmarshaling accepts both.
type UserSession struct {
	Sub        string `json:"sub"`
	ExpiresAt  int64  `json:"expires_at,omitempty"`
	CreatedAt  int64  `json:"created_at,omitempty"`
	LastSeenAt int64  `json:"last_seen_at,omitempty"`
}

// UserSessionStore supports legacy JSON {"tok":"sub"} and current {"tok":{...}}.
type UserSessionStore map[string]UserSession

func (m *UserSessionStore) UnmarshalJSON(b []byte) error {
	trim := bytes.TrimSpace(b)
	if string(trim) == "null" || len(trim) == 0 {
		*m = nil
		return nil
	}
	// Legacy: map[string]string
	var legacy map[string]string
	if err := json.Unmarshal(trim, &legacy); err == nil {
		out := make(UserSessionStore, len(legacy))
		for tok, sub := range legacy {
			out[tok] = UserSession{Sub: sub}
		}
		*m = out
		return nil
	}
	var cur map[string]UserSession
	if err := json.Unmarshal(trim, &cur); err != nil {
		return err
	}
	*m = UserSessionStore(cur)
	return nil
}
