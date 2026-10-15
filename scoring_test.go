package main

import "testing"

// กฎคะแนน: ถูกเป๊ะ +3 / ถูกผล +1 / ผิด 0 / ยังไม่จบ = ยังไม่คิด

func TestScorePredictionExact(t *testing.T) {
	pred := map[string]interface{}{"home": float64(2), "away": float64(1)}
	res := &MatchResult{Home: 2, Away: 1, Finished: true}
	p, finished := scorePrediction(pred, res)
	if !finished || p != 3 {
		t.Fatalf("expected 3 (exact), got %d finished=%v", p, finished)
	}
}

func TestScorePredictionCorrectOutcome(t *testing.T) {
	// ทาย 3-0 จริง 2-1 -> ผลชนะเหมือนกัน = 1 แต้ม
	pred := map[string]interface{}{"home": float64(3), "away": float64(0)}
	res := &MatchResult{Home: 2, Away: 1, Finished: true}
	p, finished := scorePrediction(pred, res)
	if !finished || p != 1 {
		t.Fatalf("expected 1 (correct outcome), got %d finished=%v", p, finished)
	}
}

func TestScorePredictionDrawOutcome(t *testing.T) {
	// ทาย 1-1 จริง 0-0 -> เสมอเหมือนกัน = 1 แต้ม
	pred := map[string]interface{}{"home": float64(1), "away": float64(1)}
	res := &MatchResult{Home: 0, Away: 0, Finished: true}
	p, finished := scorePrediction(pred, res)
	if !finished || p != 1 {
		t.Fatalf("expected 1 (draw=draw), got %d", p)
	}
}

func TestScorePredictionWrong(t *testing.T) {
	// ทาย 2-1 (เจ้าบ้านชนะ) จริง 1-2 (ทีมเยือนชนะ) = ผิด
	pred := map[string]interface{}{"home": float64(2), "away": float64(1)}
	res := &MatchResult{Home: 1, Away: 2, Finished: true}
	p, finished := scorePrediction(pred, res)
	if !finished || p != 0 {
		t.Fatalf("expected 0 (wrong), got %d", p)
	}
}

func TestScorePredictionNotFinished(t *testing.T) {
	pred := map[string]interface{}{"home": float64(2), "away": float64(1)}
	res := &MatchResult{Home: 0, Away: 0, Finished: false}
	_, finished := scorePrediction(pred, res)
	if finished {
		t.Fatal("expected not finished -> ไม่คิดคะแนน")
	}
}

func TestScorePredictionNoResult(t *testing.T) {
	pred := map[string]interface{}{"home": float64(2), "away": float64(1)}
	_, finished := scorePrediction(pred, nil)
	if finished {
		t.Fatal("expected nil result -> ไม่คิดคะแนน")
	}
}

func TestScorePredictionIntValues(t *testing.T) {
	// JSON บางทีมาเป็น float64 บางทีเป็น int — ต้องรองรับทั้งคู่
	pred := map[string]interface{}{"home": 1, "away": 0}
	res := &MatchResult{Home: 1, Away: 0, Finished: true}
	p, finished := scorePrediction(pred, res)
	if !finished || p != 3 {
		t.Fatalf("expected 3 with int values, got %d", p)
	}
}
