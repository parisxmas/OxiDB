package config

import "os"

type Config struct {
	HTTPAddr     string
	OxiDBHost    string
	OxiDBPort    int
	PoolSize     int
	JWTSecret    string
	AdminEmail   string
	AdminPass    string
}

func Load() *Config {
	return &Config{
		HTTPAddr:   getEnv("DMS_ADDR", ":8080"),
		OxiDBHost:  getEnv("OXIDB_HOST", "127.0.0.1"),
		OxiDBPort:  getEnvInt("OXIDB_PORT", 4444),
		PoolSize:   getEnvInt("DMS_POOL_SIZE", 3),
		JWTSecret:  getEnv("DMS_JWT_SECRET", "oxidms-dev-secret-change-me"),
		AdminEmail: getEnv("DMS_ADMIN_EMAIL", "admin@oxidms.local"),
		AdminPass:  getEnv("DMS_ADMIN_PASS", "admin123"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return fallback
		}
		n = n*10 + int(c-'0')
	}
	return n
}
