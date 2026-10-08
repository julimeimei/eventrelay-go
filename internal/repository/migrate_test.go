package repository

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMigrationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		want    int64
		wantErr bool
	}{
		{name: "valid", file: "000001_create_events.sql", want: 1},
		{name: "missing underscore", file: "000001.sql", wantErr: true},
		{name: "invalid version", file: "first_create_events.sql", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := migrationVersion(tt.file)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestLoadMigrationsSortsSQLFilesByVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "000010_ten.sql", "SELECT 10;")
	writeFile(t, dir, "000002_two.sql", "SELECT 2;")
	writeFile(t, dir, "README.md", "ignored")

	migrations, err := loadMigrations(dir)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(migrations))
	}
	if migrations[0].Version != 2 || migrations[1].Version != 10 {
		t.Fatalf("expected migrations sorted by version, got %+v", migrations)
	}
	if migrations[0].SQL != "SELECT 2;" {
		t.Fatalf("expected migration SQL to be loaded, got %q", migrations[0].SQL)
	}
}

func TestLoadMigrationsRejectsInvalidSQLFileName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "create_events.sql", "SELECT 1;")

	if _, err := loadMigrations(dir); err == nil {
		t.Fatal("expected error")
	}
}

func TestSplitSQLStatementsTrimsEmptyStatements(t *testing.T) {
	t.Parallel()

	got := splitSQLStatements(`
		CREATE TABLE events (id TEXT);

		CREATE INDEX idx_events_id ON events (id);
		;
	`)

	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %#v", len(got), got)
	}
	if got[0] != "CREATE TABLE events (id TEXT)" {
		t.Fatalf("unexpected first statement: %q", got[0])
	}
	if got[1] != "CREATE INDEX idx_events_id ON events (id)" {
		t.Fatalf("unexpected second statement: %q", got[1])
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
