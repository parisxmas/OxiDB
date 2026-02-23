package repository

import "fmt"

// normalizeID converts the _id field from numeric (float64) to string
// since OxiDB returns auto-increment numeric IDs.
func normalizeID(doc map[string]any) {
	if id, ok := doc["_id"]; ok {
		switch v := id.(type) {
		case float64:
			doc["_id"] = fmt.Sprintf("%.0f", v)
		case int:
			doc["_id"] = fmt.Sprintf("%d", v)
		}
	}
}

// extractID gets the inserted document ID from an OxiDB insert response.
func extractID(result map[string]any) string {
	if id, ok := result["id"]; ok {
		switch v := id.(type) {
		case string:
			return v
		case float64:
			return fmt.Sprintf("%.0f", v)
		}
	}
	return ""
}
