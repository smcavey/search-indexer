// Copyright Contributors to the Open Cluster Management project
package testutils

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ====================================================
// Mock the Row interface defined in the pgx library.
// https://github.com/jackc/pgx/blob/master/rows.go#L81
// ====================================================
// type MockRow struct {
// 	MockValue []interface{}
// 	MockError error
// }

// func (r *MockRow) Scan(dest ...interface{}) error {
// 	if r.MockError != nil {
// 		return r.MockError
// 	}
// 	*dest[0].(*int) = r.MockValue[0].(int)
// 	return nil
// }

// ====================================================
// Mock the Rows interface defined in the pgx library.
// https://github.com/jackc/pgx/blob/master/rows.go#L26
// ====================================================
type MockRows struct {
	MockData        []map[string]interface{}
	Index           int
	ColumnHeaders   []string
	MockErrorOnScan error
	nextCalled      bool // Track if Next() was called (used as Rows vs Row)
}

func (r *MockRows) Close() {}

func (r *MockRows) Err() error { return nil }

func (r *MockRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *MockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *MockRows) Conn() *pgx.Conn { return nil }

func (r *MockRows) Next() bool {
	r.nextCalled = true
	r.Index = r.Index + 1
	return r.Index <= len(r.MockData)
}

func (r *MockRows) Scan(dest ...interface{}) error {
	if r.MockErrorOnScan != nil {
		return r.MockErrorOnScan
	}

	// When used as Rows (via Next/Scan), Index is 1-based after Next(), use Index-1
	// When used as Row (via QueryRow/Scan), Index starts at 0 and increments after each Scan
	idx := r.Index
	if r.nextCalled {
		// Used as Rows with Next()
		idx = idx - 1
	}

	if idx < 0 || idx >= len(r.MockData) {
		return fmt.Errorf("index out of range: %d", idx)
	}

	if len(dest) == 2 { // uid and data
		*dest[0].(*string) = r.MockData[idx]["uid"].(string)
		props, _ := r.MockData[idx]["data"].(map[string]interface{})
		dest[1] = props
	} else {
		for i := range dest {
			switch v := dest[i].(type) {
			case *int:
				// for Test_ClusterTotals test
				*dest[0].(*int) = r.MockData[idx]["count"].(int)
			case *string:
				*dest[i].(*string) = r.MockData[idx][r.ColumnHeaders[i]].(string)
			case *map[string]interface{}:
				*dest[i].(*map[string]interface{}) = r.MockData[idx][r.ColumnHeaders[i]].(map[string]interface{})
			case *interface{}:
				dest[i] = r.MockData[idx][r.ColumnHeaders[i]]
			case nil:
				fmt.Printf("error type %T", v)
			default:
				fmt.Printf("unexpected type %T", v)

			}
		}
	}

	// When used as Row (QueryRow), increment Index for next call
	if !r.nextCalled {
		r.Index++
	}

	return nil
}

func (r *MockRows) Values() ([]interface{}, error) { return nil, nil }

func (r *MockRows) RawValues() [][]byte { return nil }
