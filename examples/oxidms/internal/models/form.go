package models

import "encoding/json"

// FieldDefinition is used for typed access to known field properties.
type FieldDefinition struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Type        string   `json:"type"`
	Required    bool     `json:"required,omitempty"`
	Placeholder string   `json:"placeholder,omitempty"`
	Options     []string `json:"options,omitempty"`
	Indexed     bool     `json:"indexed,omitempty"`
	MinLength   *int     `json:"minLength,omitempty"`
	MaxLength   *int     `json:"maxLength,omitempty"`
	Min         *float64 `json:"min,omitempty"`
	Max         *float64 `json:"max,omitempty"`
	X           int      `json:"x"`
	Y           int      `json:"y"`
	W           int      `json:"w"`
	H           int      `json:"h"`
}

// Form stores fields as raw maps to preserve all frontend properties (layout, etc.)
type Form struct {
	ID          string           `json:"_id,omitempty"`
	Name        string           `json:"name"`
	Slug        string           `json:"slug"`
	Description string           `json:"description,omitempty"`
	Fields      []map[string]any `json:"fields"`
	CreatedBy   string           `json:"createdBy"`
	CreatedAt   string           `json:"createdAt"`
	UpdatedAt   string           `json:"updatedAt"`
}

// TypedFields converts the raw field maps to typed FieldDefinition structs.
func (f *Form) TypedFields() []FieldDefinition {
	if len(f.Fields) == 0 {
		return nil
	}
	data, err := json.Marshal(f.Fields)
	if err != nil {
		return nil
	}
	var fields []FieldDefinition
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil
	}
	return fields
}
