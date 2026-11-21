package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAllowedImageHost(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"https://a.espncdn.com/combiner/i?img=%2Fteam%2Flogos%2Ff500%2F123.png", true},
		{"http://media.goal-api.com/images/clubs/77/logo.png", true},
		{"https://images.fotmob.com/images/logo.png", true},
		{"https://cdn.sofascore.com/images/upload/team/500.png", true},
		{"https://ichef.bbci.co.uk/ace/sport/240/cpsprodpb/5bf089a0-35ee-4f0f-882c-f9db59a1b045.jpg", true}, // news thumbnail host (evidenced)
		{"https://lh3.googleusercontent.com/abc/photo.jpg", true},
		{"https://lh4.googleusercontent.com/abc/photo.jpg", true},
		{"https://lh5.googleusercontent.com/abc/photo.jpg", true},
		{"https://lh6.googleusercontent.com/abc/photo.jpg", true},
		{"http://evil.com/image.png", false},             // ไม่อยู่ใน allowlist
		{"ftp://a.espncdn.com/image.png", false},          // scheme ไม่ถูกต้อง
		{"", false},                                       // ว่าง
		{"https://evil.espncdn.com/image.png", true},      // *.espncdn.com provider suffix ยังอนุญาต
		{"https://d1xxxxx.cloudfront.net/image.png", false}, // cloudfront wildcard กว้างเกิน/ไม่มีหลักฐานใช้งานจริง
		{"https://evil.bbci.co.uk/image.png", false},      // narrowed: เฉพาะ ichef.bbci.co.uk
		{"https://evil.googleusercontent.com/abc", false}, // narrowed: เฉพาะ lh3-6.googleusercontent.com
		{"http://googleusercontent.com/abc", false},       // narrowed: ไม่อนุญาตทั้งโดเมนกว้าง
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

func allowTestImageOrigins(orig func(string) bool, allowed ...string) func(string) bool {
	return func(raw string) bool {
		for _, p := range allowed {
			if strings.HasPrefix(raw, p) {
				return true
			}
		}
		return orig(raw)
	}
}

func TestFetchAndCacheImage_BlocksRedirectToDisallowedHost(t *testing.T) {
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("evil"))
	}))
	defer evil.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+"/img.png", http.StatusFound)
	}))
	defer good.Close()

	origValidator := imageAllowedURL
	origClient := imageHTTP
	imageAllowedURL = allowTestImageOrigins(origValidator, good.URL)
	imageHTTP = newImageHTTPClient()
	t.Cleanup(func() {
		imageAllowedURL = origValidator
		imageHTTP = origClient
	})

	if _, err := fetchAndCacheImage(good.URL + "/start.png"); err == nil {
		t.Fatal("expected redirect to disallowed host to be rejected")
	}
}

func TestFetchAndCacheImage_AllowsRedirectToAllowedHost(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("ok-image"))
	}))
	defer target.Close()

	start := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/img.png", http.StatusFound)
	}))
	defer start.Close()

	origValidator := imageAllowedURL
	origClient := imageHTTP
	imageAllowedURL = allowTestImageOrigins(origValidator, start.URL, target.URL)
	imageHTTP = newImageHTTPClient()
	t.Cleanup(func() {
		imageAllowedURL = origValidator
		imageHTTP = origClient
	})

	img, err := fetchAndCacheImage(start.URL + "/start.png")
	if err != nil {
		t.Fatalf("expected allowed redirect chain to succeed, got: %v", err)
	}
	if string(img.data) != "ok-image" {
		t.Fatalf("unexpected image data: %q", string(img.data))
	}
}

func TestFetchAndCacheImage_RejectsTooManyRedirects(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// always redirect to self path to create a loop/chain
		http.Redirect(w, r, srv.URL+fmt.Sprintf("/loop%d", 1), http.StatusFound)
	}))
	defer srv.Close()

	origValidator := imageAllowedURL
	origClient := imageHTTP
	imageAllowedURL = allowTestImageOrigins(origValidator, srv.URL)
	imageHTTP = newImageHTTPClient()
	t.Cleanup(func() {
		imageAllowedURL = origValidator
		imageHTTP = origClient
	})

	if _, err := fetchAndCacheImage(srv.URL + "/start.png"); err == nil {
		t.Fatal("expected redirect loop/too many redirects to be rejected")
	}
}
