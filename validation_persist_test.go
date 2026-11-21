package main

import (
	"strings"
	"testing"
)

func TestSaveUserFavoritesBoundsAndSanitizes(t *testing.T) {
	old := GlobalDB
	GlobalDB = &PersistentDB{data: newEmptyStoreData()}
	t.Cleanup(func() { GlobalDB = old })

	favs := make([]int, 0, maxFavoritesPerUser+10)
	for i := 0; i < maxFavoritesPerUser+10; i++ {
		favs = append(favs, i+1)
	}
	favs = append(favs, 1, -5, 0) // dup/invalid
	GlobalDB.SaveUserFavorites("u1", favs)
	got := GlobalDB.GetUserFavorites("u1")
	if len(got) != maxFavoritesPerUser {
		t.Fatalf("len=%d want=%d", len(got), maxFavoritesPerUser)
	}
	if got[0] != 1 || got[len(got)-1] != maxFavoritesPerUser {
		t.Fatalf("unexpected bounds: first=%d last=%d", got[0], got[len(got)-1])
	}
}

func TestSetUserInterestRejectsInvalidAndCaps(t *testing.T) {
	old := GlobalDB
	GlobalDB = &PersistentDB{data: newEmptyStoreData()}
	t.Cleanup(func() { GlobalDB = old })

	got := GlobalDB.SetUserInterest("u1", "league:bad\nid", true)
	if len(got) != 0 {
		t.Fatalf("expected invalid interest rejected, got %v", got)
	}
	for i := 0; i < maxInterestsPerUser+5; i++ {
		GlobalDB.SetUserInterest("u1", "team:t"+strings.Repeat("a", 2)+string(rune('a'+(i%20)))+string(rune('0'+(i%10))), true)
	}
	got = GlobalDB.GetUserInterests("u1")
	if len(got) > maxInterestsPerUser {
		t.Fatalf("len=%d cap=%d", len(got), maxInterestsPerUser)
	}
}

func TestSavePredictionRejectsOversizeAndInvalidID(t *testing.T) {
	old := GlobalDB
	GlobalDB = &PersistentDB{data: newEmptyStoreData()}
	t.Cleanup(func() { GlobalDB = old })

	if err := GlobalDB.SavePrediction("u1", "bad id", map[string]interface{}{"home": 1}); err == nil {
		t.Fatal("expected invalid match id error")
	}
	big := strings.Repeat("x", maxPredictionJSONBytes+10)
	if err := GlobalDB.SavePrediction("u1", "m1", map[string]interface{}{"note": big}); err == nil {
		t.Fatal("expected oversize prediction error")
	}
	if err := GlobalDB.SavePrediction("u1", "m1", map[string]interface{}{"home": 1, "away": 0}); err != nil {
		t.Fatalf("valid prediction rejected: %v", err)
	}
}

func TestSaveNotificationPrefsRejectsOversize(t *testing.T) {
	old := GlobalDB
	GlobalDB = &PersistentDB{data: newEmptyStoreData()}
	t.Cleanup(func() { GlobalDB = old })
	big := strings.Repeat("x", maxNotificationPrefsBytes+10)
	if err := GlobalDB.SaveNotificationPrefs("u1", map[string]interface{}{"k": big}); err == nil {
		t.Fatal("expected oversize prefs error")
	}
}

func TestAddChatReportSanitizesAndRejectsInvalid(t *testing.T) {
	old := GlobalDB
	GlobalDB = &PersistentDB{data: newEmptyStoreData()}
	t.Cleanup(func() { GlobalDB = old })
	ok := GlobalDB.AddChatReport(ChatReport{MatchID: "bad id", Reason: "spam"})
	if ok {
		t.Fatal("expected invalid match id rejected")
	}
	ok = GlobalDB.AddChatReport(ChatReport{MatchID: "m1", User: strings.Repeat("u", 200), Text: strings.Repeat("t", 500), Reason: strings.Repeat("r", 500)})
	if !ok {
		t.Fatal("expected valid sanitized report")
	}
	reports := GlobalDB.ListChatReports()
	if len(reports) != 1 {
		t.Fatalf("len=%d", len(reports))
	}
	if len([]rune(reports[0].User)) > maxChatReportUserLen || len([]rune(reports[0].Text)) > 300 || len([]rune(reports[0].Reason)) > 200 {
		t.Fatalf("report not sanitized: %+v", reports[0])
	}
}
