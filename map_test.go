package dbmap

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type EmbeddedType struct {
	Secret []byte    `db:"secret"`
	Splart time.Time `db:"splart"`
}

type testType struct {
	EmbeddedType
	Foo int16         `db:"foo"`
	Bar string        `db:"bar"`
	Dur time.Duration `db:"dur"`
}

func (target testType) check(row TestRow) error {
	if target.Foo != int16(row["foo"].(int)) {
		return fmt.Errorf("value Foo was not scanned")
	}
	if target.Bar != row["bar"].(string) {
		return fmt.Errorf("value Bar was not scanned")
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
	row := TestRow{
		"foo":    42,
		"bar":    "yep",
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
	rows := &TestRows{
		Current: -1,
		Rows: []TestRow{
			{
				"foo":    42,
				"bar":    "yep",
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
			if err := target.check(rows.Rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		i++
	}
}

func TestScanAll(t *testing.T) {
	rows := &TestRows{
		Current: -1,
		Rows: []TestRow{
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

	if len(slice) != len(rows.Rows) {
		t.Fatalf("Number of returned rows, %v,  does not match the input, %v", len(slice), len(rows.Rows))
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
	rows := &TestRows{
		Current: -1,
		Rows: []TestRow{
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

	if slice[0].Foo != rows.Rows[0]["foo"] {
		t.Fatalf("Field was not scanned")
	}
}
