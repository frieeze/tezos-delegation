package tds

// Delegation is a struct that represents a delegation
type Delegation struct {
	Timestamp string `json:"timestamp"`
	Delegator string `json:"delegator"`
	Amount    string `json:"amount"`
	Level     string `json:"level"`
	ID        string `json:"-"`
}
