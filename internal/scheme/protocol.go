package scheme

import "fmt"

// Protocol constants for the scheme wire format:
//
//	Request:  PAYMENT|<amount>
//	Response: RESPONSE|<status>|<reason>
const (
	requestPrefix = "PAYMENT"

	statusAccepted = "ACCEPTED"
	statusRejected = "REJECTED"

	reasonProcessed      = "Transaction processed"
	reasonInvalidRequest = "Invalid request"
	reasonInvalidAmount  = "Invalid amount"
	reasonCancelled      = "Cancelled"
)

// formatResponse renders a response line (without the trailing newline).
func formatResponse(status, reason string) string {
	return fmt.Sprintf("RESPONSE|%s|%s", status, reason)
}
