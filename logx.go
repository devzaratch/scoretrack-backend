package main

// logx — structured JSON logger กลาง (แผนข้อ 9: metrics + structured log)
//
// ทุกบรรทัด = 1 JSON object ทาง stdout:
//
//	{"level":"info","msg":"proxy ok","path":"/api/matches","status":200,"ts":"2026-10-21T18:30:00+07:00"}
//
// ทำไม: log.Printf เดิมเป็น free text 69 จุด — log drain / Cloud log
// แยก field ไม่ได้ (grep อย่างเดียว) บรรทัด JSON แปะ log drain (Render/Loki/
// Cloud Logging) แล้ว query ตาม field ได้ทันที ไม่ต้อง parse regex
//
// 2 รูปแบบ:
//   - logInfo("msg", "k1", v1) — structured fields สำหรับจุดสำคัญ
//     (request lifecycle, rate limit reject, provider fallback, ws)
//   - logInfof("fmt", args...) — printf-style เดิมห่อด้วย envelope JSON
//     แทน log.Printf แบบ mechanical: msg ยังอ่านออกเหมือนเดิมทุกประการ
//
// ระดับ log: LOG_LEVEL=debug|info|warn|error (default info)
//   - prod default (info): เตือน + ข้อผิดพลาดล้วน — access log ของ request
//     ปกติเป็น debug (log drain ไม่บวม, Render free tier มีโควตาจำกัด) —
//     อยากรัน full access log ตั้ง LOG_LEVEL=debug
//   - Fatal: log แล้ว os.Exit(1) เหมือน log.Fatalf เดิม

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ระดับ log: 0=debug 1=info 2=warn 3=error
var logLevelAtomic atomic.Int32

// logWriter — สลับ output ได้ตอน test (default stdout)
var logWriter io.Writer = os.Stdout

func init() {
	setLogLevel(os.Getenv("LOG_LEVEL"))
}

func setLogLevel(v string) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		logLevelAtomic.Store(0)
	case "warn", "warning":
		logLevelAtomic.Store(2)
	case "error":
		logLevelAtomic.Store(3)
	default:
		logLevelAtomic.Store(1) // info
	}
}

func logEnabled(level int) bool { return int32(level) >= logLevelAtomic.Load() }

var logLevelNames = [...]string{"debug", "info", "warn", "error"}

// logWrite — เขียน 1 บรรทัด JSON (kv = คู่ key/value สลับกัน; คี่ท้ายสุดเก็บเป็น _extra)
// กันชน field หลัก (ts/level/msg) ด้วย underscore prefix และ marshal พัง = fallback msg ล้วน
func logWrite(level int, msg string, kv ...any) {
	name := logLevelNames[level]
	m := make(map[string]any, len(kv)/2+3)
	m["ts"] = time.Now().Format(time.RFC3339Nano)
	m["level"] = name
	m["msg"] = msg
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			k = fmt.Sprint(kv[i])
		}
		if k == "" || k == "ts" || k == "level" || k == "msg" {
			k = "_" + k
		}
		m[k] = kv[i+1]
	}
	if len(kv)%2 == 1 {
		m["_extra"] = kv[len(kv)-1]
	}
	b, err := json.Marshal(m)
	if err != nil {
		b = []byte(fmt.Sprintf(`{"ts":%q,"level":%q,"msg":%q}`, time.Now().Format(time.RFC3339), name, msg))
	}
	b = append(b, '\n')
	if w := logWriter; w != nil {
		_, _ = w.Write(b)
	}
}

func logDebug(msg string, kv ...any) {
	if logEnabled(0) {
		logWrite(0, msg, kv...)
	}
}

func logInfo(msg string, kv ...any) {
	if logEnabled(1) {
		logWrite(1, msg, kv...)
	}
}

func logWarn(msg string, kv ...any) {
	if logEnabled(2) {
		logWrite(2, msg, kv...)
	}
}

func logError(msg string, kv ...any) {
	if logEnabled(3) {
		logWrite(3, msg, kv...)
	}
}

func logDebugf(format string, args ...any) {
	if logEnabled(0) {
		logWrite(0, fmt.Sprintf(format, args...))
	}
}

func logInfof(format string, args ...any) {
	if logEnabled(1) {
		logWrite(1, fmt.Sprintf(format, args...))
	}
}

func logInfoln(args ...any) {
	if logEnabled(1) {
		logWrite(1, strings.TrimRight(fmt.Sprintln(args...), "\n"))
	}
}

func logWarnf(format string, args ...any) {
	if logEnabled(2) {
		logWrite(2, fmt.Sprintf(format, args...))
	}
}

func logErrorf(format string, args ...any) {
	if logEnabled(3) {
		logWrite(3, fmt.Sprintf(format, args...))
	}
}

// logFatalf — แทน log.Fatalf เดิม (log แล้วออก 1)
func logFatalf(format string, args ...any) {
	logWrite(3, fmt.Sprintf(format, args...))
	os.Exit(1)
}
