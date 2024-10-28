package tds

import "fmt"

func ParseDelegation(csv string) Delegation {
	var d Delegation
	fmt.Sscanf(csv, "%s,%s,%s,%s\n", &d.Level, &d.Delegator, &d.Amount, &d.Timestamp)
	return d
}

// Delegation is a struct that represents a delegation
type Delegation struct {
	Timestamp string `json:"timestamp"`
	Delegator string `json:"delegator"`
	Amount    string `json:"amount"`
	Level     string `json:"level"`
	Id        string `json:"-"`
}

func (d Delegation) CSV() string {
	return fmt.Sprintf("%s,%s,%s,%s\n", d.Level, d.Delegator, d.Amount, d.Timestamp)
}
