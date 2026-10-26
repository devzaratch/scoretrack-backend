package main

import (
	"testing"
)

func resetArchive() {
	archiveMu.Lock()
	defer archiveMu.Unlock()
	archiveMem = make(map[string]map[int]ArchivedMatch)
}

func TestSeasonKeyForBangkokBoundary(t *testing.T) {
	// 2025-12-31 18:00 UTC = 2026-01-01 01:00 เวลาไทย -> ฤดูกาล 2026
	if got := seasonKeyFor("2025-12-31T18:00:00Z"); got != "2026" {
		t.Fatalf("season=%s want 2026", got)
	}
	// 2025-12-31 10:00 UTC = 17:00 เวลาไทย -> ยังเป็น 2025
	if got := seasonKeyFor("2025-12-31T10:00:00Z"); got != "2025" {
		t.Fatalf("season=%s want 2025", got)
	}
	// kickoff พัง = ปีปัจจุบัน
	if got := seasonKeyFor("garbage"); len(got) != 4 {
		t.Fatalf("fallback season=%s", got)
	}
}

func TestArchivedFromRealMatch(t *testing.T) {
	m := RealMatch{
		MatchID:   77,
		LeagueID:  47,
		League:    "Premier League",
		Country:   "England",
		Status:    "FT",
		MatchTime: "2026-05-10T15:00:00Z",
		HomeTeam:  RealTeam{Name: "Arsenal", Score: 2},
		AwayTeam:  RealTeam{Name: "Chelsea", Score: 1},
	}
	rec := archivedFromRealMatch(m)
	if rec.MatchID != 77 || rec.HomeTeam != "Arsenal" || rec.AwayTeam != "Chelsea" {
		t.Fatalf("unexpected record: %+v", rec)
	}
	if rec.HomeScore != 2 || rec.AwayScore != 1 || rec.Season != "2026" {
		t.Fatalf("unexpected scores/season: %+v", rec)
	}
	if rec.ArchivedAt == 0 {
		t.Fatal("archived_at must be set")
	}
}

func TestArchiveOnlyFinished(t *testing.T) {
	resetArchive()
	matches := []RealMatch{
		{MatchID: 1, Status: "FT", MatchTime: "2026-05-10T15:00:00Z", HomeTeam: RealTeam{Name: "A", Score: 1}, AwayTeam: RealTeam{Name: "B"}},
		{MatchID: 2, Status: "45'", MatchTime: "2026-05-10T15:00:00Z", HomeTeam: RealTeam{Name: "C"}, AwayTeam: RealTeam{Name: "D"}},
		{MatchID: 3, Status: "NS", MatchTime: "2026-05-10T18:00:00Z", HomeTeam: RealTeam{Name: "E"}, AwayTeam: RealTeam{Name: "F"}},
	}
	if n := archiveFromMatches(matches); n != 1 {
		t.Fatalf("stored=%d want 1", n)
	}
	if _, ok := findArchived("2026", 2); ok {
		t.Fatal("live match must not be archived")
	}
	if _, ok := findArchived("2026", 1); !ok {
		t.Fatal("finished match must be archived")
	}
}

func TestListArchivedSortAndLimit(t *testing.T) {
	resetArchive()
	archiveFromMatches([]RealMatch{
		{MatchID: 10, Status: "FT", MatchTime: "2026-03-01T15:00:00Z", HomeTeam: RealTeam{Name: "A", Score: 1}, AwayTeam: RealTeam{Name: "B"}},
		{MatchID: 11, Status: "FT", MatchTime: "2026-05-01T15:00:00Z", HomeTeam: RealTeam{Name: "C", Score: 2}, AwayTeam: RealTeam{Name: "D"}},
		{MatchID: 12, Status: "FT", MatchTime: "2026-04-01T15:00:00Z", HomeTeam: RealTeam{Name: "E", Score: 3}, AwayTeam: RealTeam{Name: "F"}},
	})

	list := listArchived("2026", 2)
	if len(list) != 2 {
		t.Fatalf("len=%d want 2", len(list))
	}
	if list[0].MatchID != 11 || list[1].MatchID != 12 {
		t.Fatalf("wrong order: %d, %d", list[0].MatchID, list[1].MatchID)
	}

	all := listArchived("2026", 50)
	if len(all) != 3 || all[2].MatchID != 10 {
		t.Fatalf("unexpected full list order/count: %+v", all)
	}
}

func TestArchiveUpsertKeepsLatestScore(t *testing.T) {
	resetArchive()
	archiveFromMatches([]RealMatch{{
		MatchID: 20, Status: "FT", MatchTime: "2026-06-01T15:00:00Z",
		HomeTeam: RealTeam{Name: "A", Score: 1}, AwayTeam: RealTeam{Name: "B", Score: 1},
	}})
	// ส่งข้อมูลใหม่ (แก้สกอร์ภายหลัง) — ต้องอัปเดต record เดิม ไม่เพิ่มซ้ำ
	archiveFromMatches([]RealMatch{{
		MatchID: 20, Status: "FT", MatchTime: "2026-06-01T15:00:00Z",
		HomeTeam: RealTeam{Name: "A", Score: 2}, AwayTeam: RealTeam{Name: "B", Score: 1},
	}})
	rec, ok := findArchived("2026", 20)
	if !ok || rec.HomeScore != 2 {
		t.Fatalf("expected updated score 2, got %+v (ok=%v)", rec, ok)
	}
	if got := len(listArchived("2026", 50)); got != 1 {
		t.Fatalf("duplicate record created: %d entries", got)
	}
}
