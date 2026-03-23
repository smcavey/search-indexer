// Copyright Contributors to the Open Cluster Management project
package testutils

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// RowsBuilder helps construct mock rows for testing
type RowsBuilder struct {
	columns []string
	rows    [][]interface{}
}

// NewRows creates a new RowsBuilder with the specified columns
func NewRows(columns []string) *RowsBuilder {
	return &RowsBuilder{
		columns: columns,
		rows:    make([][]interface{}, 0),
	}
}

// AddRow adds a row of values to the builder
func (rb *RowsBuilder) AddRow(values ...interface{}) *RowsBuilder {
	rb.rows = append(rb.rows, values)
	return rb
}

// ToPgxRows converts the builder to pgx.Rows using MockRows
func (rb *RowsBuilder) ToPgxRows() pgx.Rows {
	mockData := make([]map[string]interface{}, 0)
	for _, row := range rb.rows {
		rowMap := make(map[string]interface{})
		for i, col := range rb.columns {
			if i < len(row) {
				rowMap[col] = row[i]
			}
		}
		mockData = append(mockData, rowMap)
	}

	return &MockRows{
		MockData:      mockData,
		ColumnHeaders: rb.columns,
		Index:         0,
	}
}

// MockRows implements pgx.Rows interface for testing
type MockRowsImpl struct {
	MockData         []map[string]interface{}
	ColumnHeaders    []string
	Index            int
	MockErrorOnClose error
	MockErrorOnNext  error
	MockErrorOnScan  error
}

func (mr *MockRowsImpl) Close()                                       { /* no-op */ }
func (mr *MockRowsImpl) Err() error                                   { return mr.MockErrorOnNext }
func (mr *MockRowsImpl) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (mr *MockRowsImpl) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (mr *MockRowsImpl) Next() bool {
	if mr.MockErrorOnNext != nil {
		return false
	}
	if mr.Index >= len(mr.MockData) {
		return false
	}
	mr.Index++
	return true
}

func (mr *MockRowsImpl) Scan(dest ...interface{}) error {
	if mr.MockErrorOnScan != nil {
		return mr.MockErrorOnScan
	}
	// Index is 1-based after Next() increments it
	idx := mr.Index - 1
	if idx < 0 || idx >= len(mr.MockData) {
		return nil
	}

	row := mr.MockData[idx]
	for i, d := range dest {
		if i < len(mr.ColumnHeaders) {
			col := mr.ColumnHeaders[i]
			if val, ok := row[col]; ok {
				switch v := d.(type) {
				case *string:
					if str, ok := val.(string); ok {
						*v = str
					}
				case *int:
					if num, ok := val.(int); ok {
						*v = num
					}
				case *interface{}:
					*v = val
				}
			}
		}
	}
	return nil
}

func (mr *MockRowsImpl) Values() ([]interface{}, error) {
	if mr.Index == 0 || mr.Index > len(mr.MockData) {
		return nil, nil
	}
	row := mr.MockData[mr.Index-1]
	values := make([]interface{}, len(mr.ColumnHeaders))
	for i, col := range mr.ColumnHeaders {
		values[i] = row[col]
	}
	return values, nil
}

func (mr *MockRowsImpl) RawValues() [][]byte {
	return nil
}

func (mr *MockRowsImpl) Conn() *pgx.Conn {
	return nil
}
