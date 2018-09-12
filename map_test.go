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
	JSON map[string]interface{} `db:"json"`
	Dur  time.Duration          `db:"dur"`
}

func (target testType) check(row testRow) error {
	if target.Foo != int16(row["foo"].(int)) {
		return fmt.Errorf("value Foo was not scanned")
	}
	if target.Bar != row["bar"].(string) {
		return fmt.Errorf("value Bar was not scanned")
	}
	if str, ok := target.JSON["lol"]; !ok || str != "cat" {
		return fmt.Errorf("value JSON was not scanned")
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
			return fmt.Errorf("receiving column %q is nil", col)
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
	tr.current++
	return tr.current < len(tr.rows)
}

func (tr testRows) Scan(data ...interface{}) error {
	return tr.rows[tr.current].Scan(data...)
}

func testPair(mapping map[string]string, k, v string) error {
	if mapping[k] != v {
		return fmt.Errorf("unmatched pair %q, %q", k, v)
	}
	return nil
}

func TestStructMappping(t *testing.T) {
	mapping, err := StructMapping(testType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := testPair(mapping.dbToStruct, "foo", "Foo"); err != nil {
		t.Fatal(err)
	} else if err := testPair(mapping.dbToStruct, "bar", "Bar"); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	t.Log(mapping)

	target := testType{}
	if err := mapping.ScanRow(&target, row, row.Cols()...); err != nil {
		t.Fatal(err)
	}
	if err := target.check(row); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	stream := mapping.ScanStream(rows)
	var i int
	for elem := range stream {
		if err, ok := elem.(error); ok {
			t.Fatal(err)
		}
		if target, ok := elem.(testType); ok {
			if err := target.check(rows.rows[i]); err != nil {
				t.Fatal(err)
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
		t.Fatal(err)
	}

	results, err := mapping.ScanAll(rows)
	if err != nil {
		t.Fatal(err)
	}
	slice, ok := results.([]testType)
	if !ok {
		t.Fatalf("Invalid return value for ScanAll(): %v", reflect.TypeOf(slice))
	}

	if len(slice) != len(rows.rows) {
		t.Fatalf("Number of returned rows, %v,  does not match the input, %v", len(slice), len(rows.rows))
	}
}

func TestDuplicateMapping(t *testing.T) {
	type MyStruct struct {
		Foo int `db:"foo"`
		Bar int `db:"foo"`
	}
	if _, err := StructMapping(MyStruct{}); err == nil {
		t.Fatalf("expected an error")
	}
}

func TestDuplicateMappingEmbedded(t *testing.T) {
	type MyEmbeddedStruct struct {
		Foo int `db:"foo"`
	}
	type MyStruct struct {
		MyEmbeddedStruct
		Bar int `db:"foo"`
	}
	if _, err := StructMapping(MyStruct{}); err == nil {
		t.Fatalf("expected an error")
	}
}

func TestDefaultDBName(t *testing.T) {
	tt := []struct {
		FieldName string
		DBName    string
	}{
		{"Foo", "foo"},
		{"FooBar", "foo_bar"},
		{"FooBarBaz", "foo_bar_baz"},
		{"JSON", "json"},
		{"JSONThing", "json_thing"},
		{"FooJSONThing", "foo_json_thing"},
		{"FooXBar", "foo_x_bar"},
	}
	for i, tc := range tt {
		t.Run(tc.DBName, func(t *testing.T) {
			dbName := defaultDBName(tc.FieldName)
			if dbName != tc.DBName {
				t.Fatalf("unexpected dbName at index %d, exp %q, got %q", i, tc.DBName, dbName)
			}
		})
	}
}

func TestDefaultNameMapping(t *testing.T) {
	rows := &testRows{
		current: -1,
		rows: []testRow{
			{"foo": "bar"},
		},
	}

	type MyStruct struct {
		Foo string
	}

	mapping, err := StructMapping(MyStruct{})
	if err != nil {
		t.Fatal(err)
	}

	results, err := mapping.ScanAll(rows)
	if err != nil {
		t.Fatal(err)
	}
	slice, ok := results.([]MyStruct)
	if !ok {
		t.Fatalf("Invalid return value for ScanAll(): %v", reflect.TypeOf(slice))
	}

	if slice[0].Foo != rows.rows[0]["foo"] {
		t.Fatalf("Field was not scanned")
	}
}
