package json

import (
	"testing"

	"github.com/polyfloyd/dbmap"
)

func TestMappping(t *testing.T) {
	type MyStruct struct {
		JSONFoo map[string]interface{}
		JSONBar map[string]interface{}
	}

	rows := &dbmap.TestRows{
		Current: -1,
		Rows: []dbmap.TestRow{
			{
				"json_foo": `{"foo":"bar"}`,
				"json_bar": []byte(`{"foo":"bar"}`),
			},
		},
	}

	mapping, err := dbmap.StructMapping(MyStruct{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := mapping.ScanAll(rows)
	if err != nil {
		t.Fatal(err)
	}
	slice := results.([]MyStruct)

	if slice[0].JSONFoo["foo"] != "bar" {
		t.Fatalf("JSONFoo field was not scanned")
	}
	if slice[0].JSONBar["foo"] != "bar" {
		t.Fatalf("JSONBar field was not scanned")
	}
}

func TestAliasedMappping(t *testing.T) {
	type MyCustomJSONMap map[string]interface{}
	type MyStruct struct {
		JSON MyCustomJSONMap
	}

	rows := &dbmap.TestRows{
		Current: -1,
		Rows: []dbmap.TestRow{
			{"json": `{"foo":"bar"}`},
		},
	}

	mapping, err := dbmap.StructMapping(MyStruct{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := mapping.ScanAll(rows)
	if err != nil {
		t.Fatal(err)
	}
	slice := results.([]MyStruct)

	if slice[0].JSON["foo"] != "bar" {
		t.Fatalf("JSON field was not scanned")
	}
}
