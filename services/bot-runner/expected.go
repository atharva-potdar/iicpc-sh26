package main

// PriceLevel is a single price/quantity level returned by GET /orderbook.
type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity int64   `json:"quantity"`
}

// ExpectedBook is the expected orderbook state during the quiet period
// (all bots finished writing, connections still open).
type ExpectedBook struct {
	Bids []PriceLevel
	Asks []PriceLevel
}

// ComputeExpected returns the expected orderbook state after all numBots have
// completed their write loops but before their connections are closed.
//
// All sequences in sequences() are self-cleaning: every iteration consumes
// or explicitly cancels every order it places, leaving zero residual.
//
//   - basic_match:         bid@P(10) → ask@P(10) fully fills. Net: nothing resting.
//   - partial_fill_ladder: 3 bids totalling 15 units → market-sell(15) sweeps all. Net: nothing resting.
//   - cancel_correctness:  bid@P placed then cancelled; ask@P placed then cancelled. Net: nothing resting.
//
// Because bots operate in non-overlapping price bands (1000 units apart via
// basePrice), cross-bot fills are impossible regardless of numBots.
// The expected book is therefore always empty after an integral number of
// iterations, for any numBots value.
//
// If non-self-cleaning sequences are added in the future, extend this function
// to replay the sequence state machine per bot.
func ComputeExpected(_ int) ExpectedBook {
	return ExpectedBook{
		Bids: []PriceLevel{},
		Asks: []PriceLevel{},
	}
}
