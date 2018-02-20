package dbmap

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

type EmbeddedType struct {
	Secret []byte    `db:"secret"`
	Splart time.Time `db:"splart"`
}

type testType struct {
	EmbeddedType
	Foo  int16                  `db:"foo"`
	Bar  string                 `db:"bar"`
	Json map[string]interface{} `db:"json"`
	Dur  time.Duration          `db:"dur"`
}

func (target testType) check(row testRow) error {
	if target.Foo != int16(row["foo"].(int)) {
		return fmt.Errorf("value Foo was not scanned")
	}
	if target.Bar != row["bar"].(string) {
		return fmt.Errorf("value Bar was not scanned")
	}
	if str, ok := target.Json["lol"]; !ok || str != "cat" {
		return fmt.Errorf("value Json was not scanned")
	}
	if target.Dur != row["dur"].(time.Duration) {
		return fmt.Errorf("value Dur was not scanned")
	}
	if !target.Splart.Equal(row["splart"].(time.Time)) {
		return fmt.Errorf("value Splart was not scanned")
	}
	if !reflect.DeepEqual(target.Secret, row["secret"].([]byte)) {
		return fmt.Errorf("value secret was not scanned")
	}
	return nil
}

type testRow map[string]interface{}

func (row testRow) Cols() []string {
	cols := make([]string, 0, len(row))
	for c := range row {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func (row testRow) Scan(data ...interface{}) error {
	for i, col := range row.Cols() {
		if data[i] == nil {
			return fmt.Errorf("Receiving column %q is nil", col)
		}
		tar := reflect.Indirect(reflect.ValueOf(data[i]))
		tar.Set(reflect.ValueOf(row[col]).Convert(tar.Type()))
	}
	return nil
}

type testRows struct {
	rows []testRow

	// This should be initialized to -1!
	current int
}

func (testRows) Close() error {
	return nil
}

func (tr testRows) Columns() ([]string, error) {
	if len(tr.rows) > 0 {
		return tr.rows[0].Cols(), nil
	}
	return []string{}, nil
}

func (testRows) Err() error {
	return nil
}

func (tr *testRows) Next() bool {
	tr.current += 1
	return tr.current < len(tr.rows)
}

func (tr testRows) Scan(data ...interface{}) error {
	return tr.rows[tr.current].Scan(data...)
}

func testPair(mapping map[string]string, k, v string) error {
	if mapping[k] != v {
		return fmt.Errorf("Unmatched pair %q, %q", k, v)
	}
	return nil
}

func TestStructMappping(t *testing.T) {
	mapping, err := StructMapping(testType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err := testPair(mapping.dbToStruct, "foo", "Foo"); err != nil {
		t.Error(err)
	} else if err := testPair(mapping.dbToStruct, "bar", "Bar"); err != nil {
		t.Error(err)
	}
}

func TestScan(t *testing.T) {
	row := testRow{
		"foo":    42,
		"bar":    "yep",
		"json":   map[string]interface{}{"lol": "cat"},
		"dur":    time.Second * 12,
		"splart": time.Now(),
		"secret": []byte{1, 2, 3},
	}
	mapping, err := StructMapping(testType{})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(mapping)

	target := testType{}
	if err := mapping.ScanRow(&target, row, row.Cols()...); err != nil {
		t.Error(err)
		return
	}
	if err := target.check(row); err != nil {
		t.Error(err)
		return
	}
}

func TestScanStream(t *testing.T) {
	rows := &testRows{
		current: -1,
		rows: []testRow{
			{
				"foo":    42,
				"bar":    "yep",
				"json":   map[string]interface{}{"lol": "cat"},
				"dur":    time.Second * 12,
				"splart": time.Now(),
				"secret": []byte{1, 2, 3},
			},
		},
	}

	mapping, err := StructMapping(testType{})
	if err != nil {
		t.Error(err)
		return
	}

	stream := mapping.ScanStream(rows)
	var i int
	for elem := range stream {
		if err, ok := elem.(error); ok {
			t.Error(err)
			return
		}
		if target, ok := elem.(testType); ok {
			if err := target.check(rows.rows[i]); err != nil {
				t.Error(err)
				return
			}
		}
		i++
	}
}

func TestScanAll(t *testing.T) {
	rows := &testRows{
		current: -1,
		rows: []testRow{
			{"bar": "hurr durr"},
		},
	}

	mapping, err := StructMapping(testType{})
	if err != nil {
		t.Error(err)
		return
	}

	results, err := mapping.ScanAll(rows)
	if err != nil {
		t.Error(err)
		return
	}
	slice, ok := results.([]testType)
	if !ok {
		t.Errorf("Invalid return value for ScanAll(): %v", reflect.TypeOf(slice))
		return
	}

	if len(slice) != len(rows.rows) {
		t.Errorf("Number of returned rows, %v,  does not match the input, %v", len(slice), len(rows.rows))
		return
	}
}
