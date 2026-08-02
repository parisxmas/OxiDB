package models

type Document struct {
	ID           string `json:"_id,omitempty"`
	FileName     string `json:"fileName"`
	ContentType  string `json:"contentType"`
	Size         int64  `json:"size"`
	BlobKey      string `json:"blobKey"`
	FormID       string `json:"formId,omitempty"`
	SubmissionID string `json:"submissionId,omitempty"`
	UploadedBy   string `json:"uploadedBy"`
	CreatedAt    string `json:"createdAt"`
}
