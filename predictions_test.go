package main

import (
	"testing"
)

func TestPredictionSaveAndGet(t *testing.T) {
	// Initialize DB
	db := InitDB()
	GlobalDB = db

	// Save a prediction
	pred := map[string]interface{}{
		"home": 2,
		"away": 1,
	}

	db.SavePrediction("user:test", "match:999", pred)

	// Get predictions for user
	userPreds := db.GetPredictionsByUser("user:test")
	if len(userPreds) == 0 {
		t.Fatal("User should have predictions")
	}

	loaded, ok := userPreds["match:999"]
	if !ok {
		t.Fatal("Match 999 prediction not found for user")
	}

	// Cast loaded to map[string]interface{}
	loadedMap, ok := loaded.(map[string]interface{})
	if !ok {
		t.Fatal("Loaded prediction is not a map")
	}

	if loadedMap["home"] != float64(2) && loadedMap["home"] != 2 {
		t.Errorf("Expected home score 2, got %v", loadedMap["home"])
	}
}
