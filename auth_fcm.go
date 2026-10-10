package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// serviceAccountKey คือไฟล์ JSON ที่โหลดจาก Firebase Console (เก็บใน env FCM_SERVICE_ACCOUNT_JSON ห้าม commit)
type serviceAccountKey struct {
	ProjectID   string `json:"project_id"`
	PrivateKey  string `json:"private_key"`
	ClientEmail string `json:"client_email"`
	TokenURI    string `json:"token_uri"`
}

var fcmAccessCache = struct {
	sync.Mutex
	token string
	exp   time.Time
}{}

// getFCMAccessToken คืน access token สำหรับ FCM HTTP v1
// ลำดับ: FCM_ACCESS_TOKEN (ใส่เองชั่วคราว) -> mint จาก service account (cache จนหมดอายุ)
func getFCMAccessToken() string {
	if t := strings.TrimSpace(os.Getenv("FCM_ACCESS_TOKEN")); t != "" {
		return t
	}

	raw := strings.TrimSpace(os.Getenv("FCM_SERVICE_ACCOUNT_JSON"))
	if raw == "" {
		return ""
	}

	fcmAccessCache.Lock()
	defer fcmAccessCache.Unlock()

	if fcmAccessCache.token != "" && time.Now().Add(5*time.Minute).Before(fcmAccessCache.exp) {
		return fcmAccessCache.token
	}

	token, ttl, err := mintGoogleAccessToken(raw)
	if err != nil {
		log.Printf("❌ mint FCM access token ล้มเหลว: %v", err)
		return ""
	}
	fcmAccessCache.token = token
	fcmAccessCache.exp = time.Now().Add(ttl)
	return token
}

// mintGoogleAccessToken แลก access token ด้วย JWT (stdlib ล้วน ไม่มี lib เพิ่ม)
func mintGoogleAccessToken(saJSON string) (string, time.Duration, error) {
	var sa serviceAccountKey
	if err := json.Unmarshal([]byte(saJSON), &sa); err != nil {
		return "", 0, fmt.Errorf("parse service account: %w", err)
	}
	if sa.PrivateKey == "" || sa.ClientEmail == "" {
		return "", 0, fmt.Errorf("service account ขาด private_key/client_email")
	}
	tokenURI := sa.TokenURI
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil {
		return "", 0, fmt.Errorf("decode private key ไม่ได้")
	}
	var priv *rsa.PrivateKey
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		var ok bool
		priv, ok = k.(*rsa.PrivateKey)
		if !ok {
			return "", 0, fmt.Errorf("key ไม่ใช่ RSA")
		}
	} else if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		priv = k
	} else {
		return "", 0, fmt.Errorf("parse private key ไม่ได้")
	}

	now := time.Now()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]interface{}{
		"iss":   sa.ClientEmail,
		"scope": "https://www.googleapis.com/auth/firebase.messaging",
		"aud":   tokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload

	h := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, h[:])
	if err != nil {
		return "", 0, fmt.Errorf("sign JWT: %w", err)
	}
	assertion := signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)

	client := &http.Client{Timeout: 15 * time.Second}
	res, err := client.PostForm(tokenURI, form)
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token endpoint status %d", res.StatusCode)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return "", 0, err
	}
	if out.AccessToken == "" {
		return "", 0, fmt.Errorf("ไม่ได้ access token")
	}
	ttl := time.Duration(out.ExpiresIn) * time.Second
	if ttl <= 0 || ttl > time.Hour {
		ttl = 55 * time.Minute
	}
	return out.AccessToken, ttl, nil
}
