package repository

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/parisxmas/OxiDB/OxiDMS/internal/db"
	"github.com/parisxmas/OxiDB/OxiDMS/internal/models"
)

const UsersCollection = "_dms_users"

type UserRepo struct {
	pool *db.Pool
}

func NewUserRepo(pool *db.Pool) *UserRepo {
	return &UserRepo{pool: pool}
}

func (r *UserRepo) EnsureIndexes() error {
	c := r.pool.Get()
	return c.CreateUniqueIndex(UsersCollection, "email")
}

func (r *UserRepo) FindByEmail(email string) (*models.User, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(UsersCollection, map[string]any{"email": email})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToUser(doc)
}

func (r *UserRepo) FindByID(id string) (*models.User, error) {
	c := r.pool.Get()
	doc, err := c.FindOne(UsersCollection, map[string]any{"_id": toNumericID(id)})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return docToUser(doc)
}

func (r *UserRepo) Create(user *models.User) (string, error) {
	c := r.pool.Get()
	doc := map[string]any{
		"email":        user.Email,
		"passwordHash": user.PasswordHash,
		"name":         user.Name,
		"role":         user.Role,
		"createdAt":    user.CreatedAt,
	}
	result, err := c.Insert(UsersCollection, doc)
	if err != nil {
		return "", err
	}
	return extractID(result), nil
}

func docToUser(doc map[string]any) (*models.User, error) {
	normalizeID(doc)
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal user doc: %w", err)
	}
	var u models.User
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, fmt.Errorf("unmarshal user: %w", err)
	}
	return &u, nil
}

// toNumericID converts a string ID to float64 for OxiDB queries.
func toNumericID(id string) any {
	if n, err := strconv.ParseFloat(id, 64); err == nil {
		return n
	}
	return id
}
