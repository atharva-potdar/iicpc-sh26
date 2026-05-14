package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
)

const (
	validationTimeout = 5 * time.Second
	// penaltyPerLevel deducts this fraction from the correctness score for
	// each unexpected or missing price level in the orderbook.
	penaltyPerLevel = 0.1
)

// OrderbookSnapshot mirrors the GET /orderbook response schema defined in api-contract.md.
type OrderbookSnapshot struct {
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
	Timestamp int64        `json:"timestamp"`
}

// ValidationResult holds the outcome of a single orderbook correctness check.
type ValidationResult struct {
	CorrectnessScore float64
	ActualBids       int
	ActualAsks       int
	ExpectedBids     int
	ExpectedAsks     int
}

// ValidateOrderbook calls GET <endpoint> with a 5-second timeout, parses the
// response, and computes a correctness score in [0, 1] by comparing actual
// vs expected price levels.
//
// Scoring:
//   - Empty expected + empty actual            → 1.0 (perfect)
//   - Each unexpected level (in actual, not expected): -0.1
//   - Each missing level (in expected, not actual):    -0.1
//   - Score clamped to [0, 1]
//   - HTTP error / non-200 / parse failure:            0.0
func ValidateOrderbook(endpoint string, expected ExpectedBook) (ValidationResult, error) {
	result := ValidationResult{
		ExpectedBids: len(expected.Bids),
		ExpectedAsks: len(expected.Asks),
	}

	client := &http.Client{Timeout: validationTimeout}
	resp, err := client.Get(endpoint)
	if err != nil {
		return result, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("GET %s returned status %d", endpoint, resp.StatusCode)
	}

	var snapshot OrderbookSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return result, fmt.Errorf("decode orderbook: %w", err)
	}

	result.ActualBids = len(snapshot.Bids)
	result.ActualAsks = len(snapshot.Asks)
	result.CorrectnessScore = scoreOrderbook(snapshot, expected)
	return result, nil
}

// scoreOrderbook returns a score in [0, 1] based on level-by-level comparison.
func scoreOrderbook(actual OrderbookSnapshot, expected ExpectedBook) float64 {
	unexpected := countUnexpected(actual.Bids, expected.Bids) +
		countUnexpected(actual.Asks, expected.Asks)
	missing := countMissing(actual.Bids, expected.Bids) +
		countMissing(actual.Asks, expected.Asks)

	discrepancies := unexpected + missing
	if discrepancies == 0 {
		return 1.0
	}
	return math.Max(0, 1.0-float64(discrepancies)*penaltyPerLevel)
}

// countUnexpected counts levels present in actual but not in expected.
func countUnexpected(actual, expected []PriceLevel) int {
	if len(expected) == 0 {
		return len(actual)
	}
	expMap := buildLevelMap(expected)
	n := 0
	for _, lvl := range actual {
		expQty, ok := expMap[roundPrice(lvl.Price)]
		if !ok || expQty != lvl.Quantity {
			n++
		}
	}
	return n
}

// countMissing counts levels present in expected but absent or wrong in actual.
func countMissing(actual, expected []PriceLevel) int {
	if len(expected) == 0 {
		return 0
	}
	actMap := buildLevelMap(actual)
	n := 0
	for _, lvl := range expected {
		actQty, ok := actMap[roundPrice(lvl.Price)]
		if !ok || actQty != lvl.Quantity {
			n++
		}
	}
	return n
}

func buildLevelMap(levels []PriceLevel) map[float64]int64 {
	m := make(map[float64]int64, len(levels))
	for _, l := range levels {
		m[roundPrice(l.Price)] = l.Quantity
	}
	return m
}

// roundPrice normalises float64 prices to 6 decimal places to avoid
// floating-point representation artifacts during map lookups.
func roundPrice(p float64) float64 {
	return math.Round(p*1e6) / 1e6
}
