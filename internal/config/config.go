package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL       string
	HTTPPort          string
	DefaultLeaseTTL   time.Duration
	ReaperInterval    time.Duration
	MaxLeaseTTL       time.Duration
}

func Load() Config {
	return Config{
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://queueline:queueline@localhost:5432/queueline?sslmode=disable"),
		HTTPPort:        getEnv("HTTP_PORT", "8080"),
		DefaultLeaseTTL: getEnvDuration("DEFAULT_LEASE_TTL", 30*time.Second),
		MaxLeaseTTL:     getEnvDuration("MAX_LEASE_TTL", 15*time.Minute),
		ReaperInterval:  getEnvDuration("REAPER_INTERVAL", 10*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

var _ = strconv.Itoa // reserved for future int env vars
