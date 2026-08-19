package firebirdsql

import (
	"context"
	"database/sql"
	"testing"
)

func TestPreparedStatementDoesNotCancelCompletedExec(t *testing.T) {
	db, err := sql.Open("firebirdsql_createdb", GetTestDSN("test_cancel_completed_exec_"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err = db.Exec("CREATE TABLE cancel_test (number_value INTEGER)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("INSERT INTO cancel_test (number_value) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}

	for value := range 50 {
		ctx, cancel := context.WithCancel(context.Background())
		if _, err = stmt.ExecContext(ctx, value); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}

	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
}
