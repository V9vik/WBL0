package config

import (
	"log"
	"os"
	"strings"
)

type Config struct {
	DBSN string
}

func Load() (*Config, error) {
	DBSN, boolErr := os.LookupEnv("DB_DSN")
	if !boolErr || strings.TrimSpace(DBSN) == "" {
		log.Fatal("Ошибка чтения .env файла, точнее ошибка в DBSN")
	}
	return &Config{
		DBSN: DBSN,
	}, nil
}
