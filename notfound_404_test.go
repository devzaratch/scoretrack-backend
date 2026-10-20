package main

import "testing"

// ---------------------------------------------------------------------------
// แผนข้อ 5 — "404 จริง": backend ต้องตอบ 404 เมื่อทรัพยากรไม่มีอยู่จริง
// เดิม respond 200 ร่างเปล่า (หน้าเว็บ JSON.parse พัง) หรือ 200 + ข้อมูลปลอม
// (Arsenal 2-1 Chelsea / Arsenal FC / Bukayo Saka hardcode ทุก id ที่หาไม่เจอ)
// ---------------------------------------------------------------------------

// TestShouldNotFound_DetailTypes — หน้า detail ที่ไม่มีข้อมูลสำรอง -> 404 จริง
func TestShouldNotFound_DetailTypes(t *testing.T) {
	for _, dt := range []string{"match", "team", "player"} {
		if !shouldNotFound(dt, nil) {
			t.Errorf("%s ไม่มีข้อมูลสำรองจริงต้องคืน 404 จริง", dt)
		}
		if !shouldNotFound(dt, []byte{}) {
			t.Errorf("%s (byte ว่าง) ต้องคืน 404 จริง", dt)
		}
	}
}

// TestShouldNotFound_Keeps200ForLists — รายการ/ลีก ว่าง = ยัง 200
// (วันไม่มีบอล = ว่างเปล่า ไม่ใช่ 404 — หน้าแรกต้องโหลดได้ตามปกติ)
func TestShouldNotFound_Keeps200ForLists(t *testing.T) {
	for _, dt := range []string{"matches", "league", "team-squad", "team-fixtures"} {
		if shouldNotFound(dt, nil) {
			t.Errorf("%s ข้อมูลว่างต้องคง 200 ไม่ใช่ 404", dt)
		}
	}
}

// TestShouldNotFound_WithDataNever404 — มีข้อมูลจริงแล้วห้ามเป็น 404 เด็ดขาด
func TestShouldNotFound_WithDataNever404(t *testing.T) {
	for _, dt := range []string{"match", "team", "player"} {
		if shouldNotFound(dt, []byte(`{"header":{}}`)) {
			t.Errorf("%s มีข้อมูลแล้วต้องไม่ 404", dt)
		}
	}
}

// ---------------------------------------------------------------------------
// looksLikeMatchDetails — กัน JSON error ที่ RapidAPI/Proxy ตอบ 200 มา
// ปลอมเป็นแมตช์จริง (เดิมหลุดไป -> หน้าโชว์ Home/Away ปลอมแทน 404)
// ---------------------------------------------------------------------------

func TestLooksLikeMatchDetails_RealShapes(t *testing.T) {
	cases := map[string]map[string]interface{}{
		"header only":  {"header": map[string]interface{}{"teams": []interface{}{}}},
		"general only": {"general": map[string]interface{}{"matchId": "123"}},
		"content only": {"content": map[string]interface{}{}},
		"ครบทั้งสาม": {
			"header":  map[string]interface{}{},
			"general": map[string]interface{}{},
			"content": map[string]interface{}{},
		},
	}
	for name, m := range cases {
		if !looksLikeMatchDetails(m) {
			t.Errorf("รูปทรง %q ต้องถือว่าเป็น matchDetails จริง", name)
		}
	}
}

func TestLooksLikeMatchDetails_ErrorPayloads(t *testing.T) {
	cases := map[string]map[string]interface{}{
		"nil":                 nil,
		"empty object":        {},
		"error json":          {"error": true, "message": "Match not found"},
		"status error":        {"status": "error", "statusCode": 404},
		"upstream rate limit": {"message": "Too many requests"},
	}
	for name, m := range cases {
		if looksLikeMatchDetails(m) {
			t.Errorf("payload %q ต้องไม่ผ่านเป็น matchDetails (กันข้อมูลปลอมหลุด)", name)
		}
	}
}
