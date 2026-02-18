package service

import (
	"errors"
	"time"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/auth"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/repository"
)

type AuthService struct {
	users     *repository.UserRepo
	jwtSecret string
}

func NewAuthService(users *repository.UserRepo, jwtSecret string) *AuthService {
	return &AuthService{users: users, jwtSecret: jwtSecret}
}

type AuthResult struct {
	Token string              `json:"token"`
	User  models.UserResponse `json:"user"`
}

func (s *AuthService) Register(email, password, name string) (*AuthResult, error) {
	existing, _ := s.users.FindByEmail(email)
	if existing != nil {
		return nil, errors.New("email already registered")
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, err
	}
	user := &models.User{
		Email:        email,
		PasswordHash: hash,
		Name:         name,
		Role:         "user",
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	id, err := s.users.Create(user)
	if err != nil {
		return nil, err
	}
	user.ID = id
	token, err := auth.GenerateToken(s.jwtSecret, id, email, user.Role)
	if err != nil {
		return nil, err
	}
	return &AuthResult{Token: token, User: user.ToResponse()}, nil
}

func (s *AuthService) Login(email, password string) (*AuthResult, error) {
	user, err := s.users.FindByEmail(email)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, errors.New("invalid credentials")
	}
	if !auth.CheckPassword(password, user.PasswordHash) {
		return nil, errors.New("invalid credentials")
	}
	token, err := auth.GenerateToken(s.jwtSecret, user.ID, user.Email, user.Role)
	if err != nil {
		return nil, err
	}
	return &AuthResult{Token: token, User: user.ToResponse()}, nil
}

func (s *AuthService) Me(userID string) (*models.UserResponse, error) {
	user, err := s.users.FindByID(userID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, errors.New("user not found")
	}
	resp := user.ToResponse()
	return &resp, nil
}

func (s *AuthService) SeedAdmin(email, password string) error {
	existing, _ := s.users.FindByEmail(email)
	if existing != nil {
		return nil
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	user := &models.User{
		Email:        email,
		PasswordHash: hash,
		Name:         "Admin",
		Role:         "admin",
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	_, err = s.users.Create(user)
	return err
}
