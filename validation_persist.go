package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	maxFavoritesPerUser        = 200
	maxInterestIDLen           = 128
	maxInterestsPerUser        = 200
	maxPredictionJSONBytes     = 4096
	maxPredictionMatchIDLen    = 64
	maxNotificationPrefsBytes  = 4096
	maxFCMTokenLen             = 4096
	maxMatchSubscriptions     = 5000
	maxSubscriptionsPerMatch   = 500
	maxProfileNameLen          = 80
	maxProfileEmailLen         = 254
	maxPasswordHashLen         = 255
	maxChatReportUserLen       = 80
	maxChatMatchIDLen          = 64
)

func trimToRuneLen(s string, max int) string {
	s = strings.TrimSpace(s)
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}

func normalizeEmailForStore(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func validInterestID(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > maxInterestIDLen {
		return false
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':' || r == ' ' {
			continue
		}
		return false
	}
	return true
}

func sanitizeFavorites(favs []int, max int) []int {
	if max <= 0 {
		max = maxFavoritesPerUser
	}
	out := make([]int, 0, minInt(len(favs), max))
	seen := map[int]bool{}
	for _, id := range favs {
		if id <= 0 || id > 1_000_000_000 {
			continue
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
		if len(out) >= max {
			break
		}
	}
	return out
}

func validateJSONObjectSize(v map[string]interface{}, maxBytes int, label string) error {
	if v == nil {
		return fmt.Errorf("%s missing", label)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("%s invalid", label)
	}
	if len(b) > maxBytes {
		return fmt.Errorf("%s too large", label)
	}
	return nil
}

func sanitizeMatchID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) > maxPredictionMatchIDLen {
		return ""
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == ':' {
			continue
		}
		return ""
	}
	return id
}

func sanitizeFCMToken(token string) string {
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxFCMTokenLen {
		return ""
	}
	return token
}
