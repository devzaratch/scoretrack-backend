package main

import "testing"

// TestImageAllowedHost — ホストホワイトリスト検証 (並列不可: テスト独立のため逐次実行)
func TestAllowedImageHost(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"https://a.espncdn.com/combiner/i?img=%2Fteam%2Flogos%2Ff500%2F123.png", true},
		{"http://media.goal-api.com/images/clubs/77/logo.png", true},
		{"https://images.fotmob.com/images/logo.png", true},
		{"https://cdn.sofascore.com/images/upload/team/500.png", true},
		{"https://lh3.googleusercontent.com/abc/photo.jpg", true},
		{"https://d1xxxxx.cloudfront.net/image.png", true},
		{"http://evil.com/image.png", false},       // ไม่อยู่ใน allowlist
		{"ftp://a.espncdn.com/image.png", false},    // scheme ไม่ถูกต้อง
		{"", false},                                 // ว่าง
		{"https://evil.espncdn.com/image.png", true}, // *.espncdn.com ก็อนุญาต (suffix match)
		{"http://googleusercontent.com/abc", true},
	}

	for _, tc := range cases {
		if got := allowedImageURL(tc.url); got != tc.want {
			t.Errorf("allowedImageURL(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}

func TestImageCacheKeyDeterministic(t *testing.T) {
	k1 := imageCacheKey("https://a.espncdn.com/logo.png")
	k2 := imageCacheKey("https://a.espncdn.com/logo.png")
	k3 := imageCacheKey("https://other.com/logo.png")
	if k1 != k2 {
		t.Fatal("cache key ต้องคงที่สำหรับ URL เดียวกัน")
	}
	if k1 == k3 {
		t.Fatal("cache key ต้องต่างกันสำหรับ URL คนละอัน")
	}
	if len(k1) != 64 {
		t.Fatalf("cache key ต้องยาว 64 hex chars, ได้ %d", len(k1))
	}
}

func TestParseCachedImage(t *testing.T) {
	payload := append([]byte("image/webp\n"), []byte("fake-webp-bytes")...)
	img, ok := parseCachedImage(payload)
	if !ok || img.contentType != "image/webp" || string(img.data) != "fake-webp-bytes" {
		t.Fatalf("parseCachedImage ผิด: %+v ok=%v", img, ok)
	}
	if _, ok := parseCachedImage([]byte("not-an-image\n")); ok {
		t.Fatal("content-type ไม่ใช่ image/ ต้อง fail")
	}
	if _, ok := parseCachedImage([]byte("nodata")); ok {
		t.Fatal("payload ขาด \n ต้อง fail")
	}
}
