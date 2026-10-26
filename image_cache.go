package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Image Cache / CDN (แผนใหม่ข้อ 6)
// โลโก้ทีม/รูปนักเตะมาจาก provider โดยตรง -> ช้า, โดน rate limit, URL เปลี่ยนแล้วรูปพัง
// ทางแก้: proxy ผ่าน backend + cache 7 วัน (memory + Redis) + จำกัด host/ขนาด/content-type
// ใช้: /api/img?u=<encoded original url>  (ผ่าน Next proxy เดิม ไม่ต้องเพิ่ม route ฝั่ง web)
//
// ความปลอดภัย: allowlist host เท่านั้น (กัน SSRF), จำกัด 2MB, เฉพาะ image/*

const (
	imageCacheTTL       = 7 * 24 * time.Hour
	imageMaxBytes       = 2 << 20 // 2 MB
	imageMemoryCap      = 250
	imageFetchTimeout   = 6 * time.Second
	imageRedisPrefix    = "img:"
	imageDefaultQuality = 80 // สงวนไว้สำหรับอนาคต (ตอนนี้ provider ส่ง webp/png มาแล้ว)
)

var imageAllowedSuffixes = []string{
	"espncdn.com",
	"goal-api.com",
	"fotmob.com",
	"sofascore.com",
	"googleusercontent.com",
	"cloudfront.net",
}

type cachedImage struct {
	data        []byte
	contentType string
	storedAt    time.Time
}

var (
	imageMu   sync.Mutex
	imageMem  = make(map[string]cachedImage)
	imageHTTP = &http.Client{Timeout: imageFetchTimeout}
)

// imageCacheKey — sha256 ของ URL (ชื่อไฟล์สั้น คงที่ ไม่รั่ว URL ยาวใน Redis key)
func imageCacheKey(rawURL string) string {
	sum := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(sum[:])
}

// allowedImageURL — ตรวจ host กับ allowlist + scheme http/https เท่านั้น
func allowedImageURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, suffix := range imageAllowedSuffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

// parseCachedImage — แปลง payload จาก Redis: "<content-type>\n<bytes>"
func parseCachedImage(payload []byte) (cachedImage, bool) {
	idx := strings.IndexByte(string(payload), '\n')
	if idx <= 0 || idx+1 >= len(payload) {
		return cachedImage{}, false
	}
	ct := string(payload[:idx])
	if !strings.HasPrefix(ct, "image/") {
		return cachedImage{}, false
	}
	data := payload[idx+1:]
	if len(data) == 0 || len(data) > imageMaxBytes {
		return cachedImage{}, false
	}
	return cachedImage{data: data, contentType: ct, storedAt: time.Now()}, true
}

// imageMemoryGet / imageMemoryPut — LRU แบบง่าย (ตัดตัวเก่าสุดเมื่อเต็ม)
func imageMemoryGet(key string) (cachedImage, bool) {
	imageMu.Lock()
	defer imageMu.Unlock()
	img, ok := imageMem[key]
	if !ok {
		return cachedImage{}, false
	}
	if time.Since(img.storedAt) > imageCacheTTL {
		delete(imageMem, key)
		return cachedImage{}, false
	}
	return img, true
}

func imageMemoryPut(key string, img cachedImage) {
	imageMu.Lock()
	defer imageMu.Unlock()
	if len(imageMem) >= imageMemoryCap {
		var oldestKey string
		var oldest time.Time = time.Now()
		for k, v := range imageMem {
			if v.storedAt.Before(oldest) {
				oldest = v.storedAt
				oldestKey = k
			}
		}
		if oldestKey != "" {
			delete(imageMem, oldestKey)
		}
	}
	imageMem[key] = img
}

// fetchAndCacheImage — ดึงรูปจาก provider แล้วเก็บ cache (memory + Redis)
func fetchAndCacheImage(rawURL string) (cachedImage, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return cachedImage{}, err
	}
	// บาง CDN บล็อก request ที่ไม่มี UA
	req.Header.Set("User-Agent", "ScoreTrack-ImageCache/1.0")
	req.Header.Set("Accept", "image/avif,image/webp,image/png,image/*;q=0.8")

	res, err := imageHTTP.Do(req)
	if err != nil {
		return cachedImage{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return cachedImage{}, fmt.Errorf("status %d", res.StatusCode)
	}
	ct := res.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "image/") {
		return cachedImage{}, fmt.Errorf("content-type ไม่ใช่รูป: %s", ct)
	}
	limited := io.LimitReader(res.Body, imageMaxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return cachedImage{}, err
	}
	if len(data) > imageMaxBytes {
		return cachedImage{}, fmt.Errorf("รูปใหญ่เกิน %d bytes", imageMaxBytes)
	}
	if len(data) == 0 {
		return cachedImage{}, fmt.Errorf("รูปว่าง")
	}

	img := cachedImage{data: data, contentType: ct, storedAt: time.Now()}
	key := imageCacheKey(rawURL)
	imageMemoryPut(key, img)
	if rdb != nil {
		payload := append([]byte(ct+"\n"), data...)
		if err := rdb.Set(ctx, imageRedisPrefix+key, payload, imageCacheTTL).Err(); err != nil {
			logWarn("image cache: Redis SET ไม่สำเร็จ", "error", err.Error())
		}
	}
	return img, nil
}

// loadCachedImage — memory -> Redis -> fetch
func loadCachedImage(rawURL string) (cachedImage, string, error) {
	key := imageCacheKey(rawURL)
	if img, ok := imageMemoryGet(key); ok {
		return img, "memory", nil
	}
	if rdb != nil {
		if payload, err := rdb.Get(ctx, imageRedisPrefix+key).Bytes(); err == nil && len(payload) > 0 {
			if img, ok := parseCachedImage(payload); ok {
				imageMemoryPut(key, img)
				return img, "redis", nil
			}
		}
	}
	img, err := fetchAndCacheImage(rawURL)
	return img, "miss", err
}

// handleImageProxy — GET /api/img?u=<encoded url>
func handleImageProxy(c *gin.Context) {
	if !envBool("IMAGE_PROXY", true) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "image proxy ปิดอยู่"})
		return
	}
	raw := c.Query("u")
	if raw == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ต้องระบุ u (image url)"})
		return
	}
	if !allowedImageURL(raw) {
		c.JSON(http.StatusForbidden, gin.H{"error": "host ไม่อยู่ในรายการที่อนุญาต"})
		return
	}
	img, source, err := loadCachedImage(raw)
	if err != nil {
		// fallback: ส่ง 404 ให้ client ใช้ placeholder (ไม่ retry ถี่)
		logWarn("image cache: ดึงรูปไม่สำเร็จ", "error", err.Error(), "url", raw)
		c.JSON(http.StatusNotFound, gin.H{"error": "ดึงรูปไม่สำเร็จ"})
		return
	}
	c.Header("Cache-Control", fmt.Sprintf("public, max-age=%d, immutable", int(imageCacheTTL.Seconds())))
	c.Header("X-Image-Cache", source)
	c.Header("Content-Length", strconv.Itoa(len(img.data)))
	c.Data(http.StatusOK, img.contentType, img.data)
}
