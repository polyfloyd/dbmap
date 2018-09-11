package dbmap

import (
	"database/sql"
	"reflect"
	"time"
)

func init() {
	RegisterMapper(nativeMapper{})
	RegisterMapper(sqlScannerMapper{})
}

type nativeMapper struct{}

func (nativeMapper) Accepts(fieldType reflect.Type) bool {
	kind := fieldType.Kind()
	if kind == reflect.Ptr {
		kind = fieldType.Elem().Kind()
	}
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64, reflect.Bool, reflect.String:
		return true
	}

	if fieldType.ConvertibleTo(reflect.TypeOf(time.Time{})) {
		return true
	}
	if fieldType.ConvertibleTo(reflect.TypeOf(&time.Time{})) {
		return true
	}
	if fieldType.ConvertibleTo(reflect.TypeOf([]byte{})) {
		return true
	}
	return false
}

func (nativeMapper) Receive(field reflect.Value) (receiver interface{}) {
	return field.Addr().Interface()
}

func (nativeMapper) Copy(target, scanned interface{}) {}

type sqlScannerMapper struct{}

func (sqlScannerMapper) Accepts(fieldType reflect.Type) bool {
	scannerType := reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	return reflect.PtrTo(fieldType).Implements(scannerType)
}

func (sqlScannerMapper) Receive(field reflect.Value) (receiver interface{}) {
	return field.Addr().Interface().(sql.Scanner)
}

func (sqlScannerMapper) Copy(target, scanned interface{}) {}
