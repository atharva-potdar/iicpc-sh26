package main

import (
	"fmt"
)

type StepKind int

const (
	StepOrder StepKind = iota
	StepCancel
)

type Step struct {
	Kind      StepKind
	Side      string
	OrderType string
	Price     float64
	Quantity  int64
	Tag       string
	CancelTag string
}

type Sequence struct {
	Name  string
	Steps []Step
}

func orderID(botID int, iteration int, tag string) string {
	return fmt.Sprintf("b%d-i%d-%s", botID, iteration, tag)
}

// basePrice gives each bot a unique price band 1000 units apart.
// Bot 0 trades at 1000, bot 1 at 2000, etc.
// Bands are wide enough that ladder sequences (±2) never overlap.
func basePrice(botID int) float64 {
	return float64((botID + 1) * 1000)
}

func sequences(botID int) []Sequence {
	return []Sequence{
		basicMatch(botID),
		partialFillLadder(botID),
		cancelCorrectness(botID),
	}
}

func basicMatch(botID int) Sequence {
	p := basePrice(botID)
	return Sequence{
		Name: "basic_match",
		Steps: []Step{
			{
				Kind:      StepOrder,
				Tag:       "bid1",
				Side:      "buy",
				OrderType: "limit",
				Price:     p,
				Quantity:  10,
			},
			{
				Kind:      StepOrder,
				Tag:       "ask1",
				Side:      "sell",
				OrderType: "limit",
				Price:     p,
				Quantity:  10,
			},
		},
	}
}

func partialFillLadder(botID int) Sequence {
	p := basePrice(botID)
	return Sequence{
		Name: "partial_fill_ladder",
		Steps: []Step{
			{
				Kind:      StepOrder,
				Tag:       "bid-0",
				Side:      "buy",
				OrderType: "limit",
				Price:     p,
				Quantity:  5,
			},
			{
				Kind:      StepOrder,
				Tag:       "bid-1",
				Side:      "buy",
				OrderType: "limit",
				Price:     p - 1,
				Quantity:  5,
			},
			{
				Kind:      StepOrder,
				Tag:       "bid-2",
				Side:      "buy",
				OrderType: "limit",
				Price:     p - 2,
				Quantity:  5,
			},
			{
				Kind:      StepOrder,
				Tag:       "sweep",
				Side:      "sell",
				OrderType: "market",
				Quantity:  15,
			},
		},
	}
}

func cancelCorrectness(botID int) Sequence {
	p := basePrice(botID)
	return Sequence{
		Name: "cancel_correctness",
		Steps: []Step{
			{
				Kind:      StepOrder,
				Tag:       "resting",
				Side:      "buy",
				OrderType: "limit",
				Price:     p,
				Quantity:  10,
			},
			{
				Kind:      StepCancel,
				CancelTag: "resting",
			},
			{
				Kind:      StepOrder,
				Tag:       "probe",
				Side:      "sell",
				OrderType: "limit",
				Price:     p,
				Quantity:  10,
			},
			{
				Kind:      StepCancel,
				CancelTag: "probe",
			},
		},
	}
}
