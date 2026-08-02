package models

type User struct {
	ID           string `json:"_id,omitempty"`
	Email        string `json:"email"`
	PasswordHash string `json:"passwordHash,omitempty"`
	Name         string `json:"name"`
	Role         string `json:"role"`
	CreatedAt    string `json:"createdAt"`
}

type UserResponse struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	CreatedAt string `json:"createdAt"`
}

func (u *User) ToResponse() UserResponse {
	return UserResponse{
		ID:        u.ID,
		Email:     u.Email,
		Name:      u.Name,
		Role:      u.Role,
		CreatedAt: u.CreatedAt,
	}
}
