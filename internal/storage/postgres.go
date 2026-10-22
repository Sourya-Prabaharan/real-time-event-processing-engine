package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Connect opens a connection pool to PostgreSQL. Uses pgx as the underlying
// driver (via database/sql compatibility layer) for better performance
// than lib/pq while keeping the standard database/sql interface.
func Connect(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return db, nil
}

// RunMigrations applies SQL migration files from the given directory.
// This is a simple sequential migrator — it reads all .up.sql files in
// alphabetical order and executes them. For a production system you'd use
// a dedicated migration tool, but this keeps the dependency count low
// and is sufficient for this project.
func RunMigrations(ctx context.Context, db *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		data, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, entry.Name()))
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("execute %s: %w", entry.Name(), err)
		}
	}
	return nil
}
