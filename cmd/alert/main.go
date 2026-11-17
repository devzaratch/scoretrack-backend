package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// main — exit code ที่ workflow/สคริปต์ใช้ตัดสินใจต่อ:
//
//	0 = ส่งสำเร็จ หรือไม่มีช่องทางให้ส่ง (skip)
//	1 = ตั้งช่องทางแล้วแต่ส่งไม่สำเร็จ หรือ -strict แล้วยังไม่ได้ตั้ง secret
//	2 = ใช้คำสั่งผิด (ไม่มี -title)
func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
}

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("alert", flag.ContinueOnError)
	fs.SetOutput(stderr)
	title := fs.String("title", "", "หัวข้อแจ้งเตือน (จำเป็น)")
	body := fs.String("body", "", "รายละเอียด (ใส่ \\n ได้)")
	level := fs.String("level", LevelInfo, "ระดับ: down | recovery | info")
	strict := fs.Bool("strict", false, "exit 1 ถ้ายังไม่ได้ตั้งช่องทางแจ้งเตือน")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*title) == "" {
		fmt.Fprintln(stderr, "alert: ต้องระบุ -title (เช่น -title \"🔴 backend ไม่ตอบ\")")
		return 2
	}

	cfg := loadConfig(getenv)
	cfg.Log = stderr

	if !cfg.Enabled() {
		fmt.Fprintln(stderr, "alert: ยังไม่ได้ตั้งช่องทางแจ้งเตือน — ตั้ง ALERT_TELEGRAM_TOKEN + ALERT_TELEGRAM_CHAT_ID หรือ ALERT_WEBHOOK_URL")
		if *strict {
			return 1
		}
		return 0
	}

	msg := Message{Title: *title, Body: *body, Level: *level}
	if err := cfg.Send(context.Background(), msg); err != nil {
		fmt.Fprintln(stderr, "alert: ส่งไม่สำเร็จ:", cfg.sanitize(err.Error()))
		return 1
	}
	return 0
}
