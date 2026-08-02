package models

type Submission struct {
	ID          string         `json:"_id,omitempty"`
	FormID      string         `json:"formId"`
	Data        map[string]any `json:"data"`
	Files       []string       `json:"files,omitempty"` // document IDs
	CreatedBy   string         `json:"createdBy"`
	CreatedAt   string         `json:"createdAt"`
	UpdatedAt   string         `json:"updatedAt"`
}
