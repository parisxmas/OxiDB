package oxidb

import "fmt"

// Error is returned when the OxiDB server returns an error response.
type Error struct {
	Msg string
}

func (e *Error) Error() string {
	return fmt.Sprintf("oxidb: %s", e.Msg)
}

// TransactionConflictError is returned on OCC version conflict during commit.
type TransactionConflictError struct {
	Msg string
}

func (e *TransactionConflictError) Error() string {
	return fmt.Sprintf("oxidb: transaction conflict: %s", e.Msg)
}
