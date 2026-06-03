package database

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func Init(ctx context.Context) (*pgxpool.Pool, error) {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	dbpool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))

	if err != nil {
		log.Printf("Unable to create connection pool: %v\n", err)
		return nil, err
	}

	_, err = dbpool.Exec(ctx, `
    	CREATE TABLE IF NOT EXISTS search_history (
			id BIGSERIAL PRIMARY KEY,
			query_pattern VARCHAR(255) NOT NULL,
			directory_path TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
	`)

	if err != nil {
		log.Printf("Unable to create table: %v\n", err)
		return nil, err
	}
	return dbpool, nil
}

func Insert(dbCon *pgxpool.Pool, query string, path string, ctx context.Context) {
	insertSQL := `INSERT INTO search_history (query_pattern, directory_path) VALUES ($1, $2);`
	_, err := dbCon.Exec(ctx, insertSQL, query, path)
	if err != nil {
		log.Println("failed to insert user:", err)
	}

	log.Println("Successfully inserted search_history", query, path)
}
