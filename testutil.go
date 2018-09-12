package dbmap

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
)

type TestRow map[string]interface{}

func (row TestRow) Cols() []string {
	cols := make([]string, 0, len(row))
	for c := range row {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func (row TestRow) Scan(data ...interface{}) error {
	for i, col := range row.Cols() {
		if data[i] == nil {
			return fmt.Errorf("receiving column %q is nil", col)
		}

		if scanner, ok := data[i].(sql.Scanner); ok {
			scanner.Scan(row[col])
			continue
		}
		tar := reflect.Indirect(reflect.ValueOf(data[i]))
		tar.Set(reflect.ValueOf(row[col]).Convert(tar.Type()))
	}
	return nil
}

type TestRows struct {
	Rows []TestRow

	// This should be initialized to -1!
	Current int
}

func (TestRows) Close() error {
	return nil
}

func (tr TestRows) Columns() ([]string, error) {
	if len(tr.Rows) > 0 {
		return tr.Rows[0].Cols(), nil
	}
	return []string{}, nil
}

func (TestRows) Err() error {
	return nil
}

func (tr *TestRows) Next() bool {
	tr.Current++
	return tr.Current < len(tr.Rows)
}

func (tr TestRows) Scan(data ...interface{}) error {
	return tr.Rows[tr.Current].Scan(data...)
}
