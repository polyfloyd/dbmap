package dbmap

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	rowScanIndexRe = regexp.MustCompile(`index (\d+): (.+)$`)
	fieldNameRe    = regexp.MustCompile(`([A-Z]+)([^A-Z]*)`)
)

var mappers []Mapper

type Mapper interface {
	// Checks whether this mapper is able to handle the specified type.
	Accepts(typ reflect.Type) bool

	// Prepare a receiving variable to scan into. The preparation function may
	// use the struct's field instead to prevent copying the receiver to the
	// struct. rows.Scan() should be able to populate the receiving values with
	// the queried data.
	// This function should return a value that can be scanned by rows.Scan().
	Receive(field reflect.Value) (receiver interface{})

	// Copy the value of the receiver to the struct's field.
	Copy(target, scanned interface{})
}

func RegisterMapper(mapper Mapper) {
	mappers = append([]Mapper{mapper}, mappers...)
}

// A Mapping is translates queried database rows to annotated structs.
type Mapping struct {
	structType reflect.Type

	// A map where the keys are the names of the database columns and the
	// values the names of the structfields.
	dbToStruct map[string]string

	// The mappers that will be used for each field.
	mapping map[string]Mapper

	// Looks up up the struct the column is a member of. This is used to
	// traverse nested structs.
	scanNesting map[string]func(struc reflect.Value) (nestedStruct reflect.Value)
}

// StructMapping creates the mapping for the specified struct.
func StructMapping(struc interface{}) (Mapping, error) {
	structType := reflect.TypeOf(struc)
	if structType.Kind() != reflect.Struct {
		return Mapping{}, fmt.Errorf("argument is not a struct, actually is %v", structType.Kind())
	}

	mapping := Mapping{
		structType:  structType,
		dbToStruct:  map[string]string{},
		mapping:     map[string]Mapper{},
		scanNesting: map[string]func(reflect.Value) reflect.Value{},
	}
	noNesting := func(s reflect.Value) reflect.Value {
		return s
	}
	if err := mapping.mapStruct(mapping.structType, noNesting); err != nil {
		return Mapping{}, err
	}
	return mapping, nil
}

// MustStructMapping is like StructMapping, but panics if an error occurs.
// Usefull for one-time initialization at the start of the program.
func MustStructMapping(struc interface{}) Mapping {
	mapping, err := StructMapping(struc)
	if err != nil {
		panic(err)
	}
	return mapping
}

func (mapping *Mapping) mapStruct(structType reflect.Type, nesting func(reflect.Value) reflect.Value) error {
outer:
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		dbName := field.Tag.Get("db")

		if dbName == "-" {
			// The field is explicitly marked to not be mapped, skip it.
			continue
		}

		if dbName == "" {
			// No name set? Check whether this is an embedded field and
			// recursively map all of its fields.
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				mapping.mapStruct(field.Type, func(s reflect.Value) reflect.Value {
					return nesting(s).FieldByName(field.Name)
				})
				continue
			}

			// The field is not an embedded struct and no name is set, infer
			// the db name from the struct field name.
			dbName = defaultDBName(field.Name)
		}

		if _, ok := mapping.dbToStruct[dbName]; ok {
			return fmt.Errorf("duplicate mapping for %q on %v", dbName, mapping.structType)
		}
		mapping.dbToStruct[dbName] = field.Name
		mapping.scanNesting[field.Name] = nesting
		for _, mapper := range mappers {
			if mapper.Accepts(field.Type) {
				mapping.mapping[field.Name] = mapper
				continue outer
			}
		}
		return fmt.Errorf("unsupported field: %v (type=%v)", field.Name, field.Type)
	}
	return nil
}

// ScanRow scans the current value of the row into the target struct.
func (mapping Mapping) ScanRow(target interface{}, row Row, scanOrder ...string) error {
	if t := reflect.TypeOf(target).Elem(); !mapping.structType.ConvertibleTo(t) {
		return fmt.Errorf("mapping type (%v) is not convertible to the scan target (%v)", mapping.structType, t)
	}

	tarval := reflect.Indirect(reflect.ValueOf(target))

	scan := make([]interface{}, len(scanOrder))
	for i, col := range scanOrder {
		strucName, ok := mapping.dbToStruct[col]
		if !ok {
			continue
		}
		field := mapping.scanNesting[strucName](tarval).FieldByName(strucName)
		scan[i] = mapping.mapping[strucName].Receive(field)
	}

	if err := row.Scan(scan...); err != nil {
		if m := rowScanIndexRe.FindStringSubmatch(err.Error()); m != nil {
			index, _ := strconv.Atoi(m[1])
			return fmt.Errorf("scan error on index %v: %v (recv: %v)", index, m[2], reflect.TypeOf(scan[index]))
		}
		return err
	}

	for i, col := range scanOrder {
		strucName, ok := mapping.dbToStruct[col]
		if !ok {
			continue
		}
		mapping.mapping[strucName].Copy(mapping.scanNesting[strucName](tarval).FieldByName(strucName).Addr().Interface(), scan[i])
	}
	return nil
}

// ScanOne scans the next row into the target. The database cursor is then
// closed, even if an error occurs.
func (mapping Mapping) ScanOne(target interface{}, rows Rows) (bool, error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	if !rows.Next() {
		return false, nil
	}
	if err := mapping.ScanRow(target, rows, cols...); err != nil {
		return false, err
	}
	return true, nil
}

// ScanStream proceeds to scan each row, sending it over the returned channel.
// If an error occurs, the sent value will be of type error and the channel
// will be closed.  The channel and rows will be closed by the sending routine.
func (mapping Mapping) ScanStream(rows Rows) <-chan interface{} {
	out := make(chan interface{})
	go func() {
		defer close(out)
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			out <- err
			return
		}

		for rows.Next() {
			scan := reflect.New(mapping.structType)
			if err := mapping.ScanRow(scan.Interface(), rows, cols...); err != nil {
				out <- err
				return
			}
			out <- reflect.Indirect(scan).Interface()
		}
		if err := rows.Err(); err != nil {
			out <- err
		}
	}()
	return out
}

// ScanAll scans all available rows into a slice. The result is returned as a
// slice of the type that was used to create this mapping. The cursor is always
// closed.  If an error occurs, none of the scanned values are returned.
func (mapping Mapping) ScanAll(rows Rows) (interface{}, error) {
	stream := mapping.ScanStream(rows)
	slice := reflect.MakeSlice(reflect.SliceOf(mapping.structType), 0, 1)
	for elem := range stream {
		if err, ok := elem.(error); ok {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(elem))
	}
	return slice.Interface(), nil
}

func (mapping Mapping) String() string {
	mapperStrings := make([]string, 0, len(mapping.mapping))
	for col, mapper := range mapping.mapping {
		mapperStrings = append(mapperStrings, fmt.Sprintf("%s: %v", col, reflect.TypeOf(mapper)))
	}
	return fmt.Sprintf("Mapping(%v){%s}", mapping.structType, strings.Join(mapperStrings, ", "))
}

type Row interface {
	Scan(dest ...interface{}) error
}

var _ Row = &sql.Row{}

type Rows interface {
	Row

	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
}

var _ Rows = &sql.Rows{}

func defaultDBName(fieldName string) string {
	matches := fieldNameRe.FindAllStringSubmatch(fieldName, -1)
	var parts []string
	for _, m := range matches {
		head, tail := m[1], m[2]
		if len(head) > 1 && len(tail) != 0 {
			i := len(head) - 1
			parts = append(parts, strings.ToLower(head[:i]))
			head = head[i:]
		}
		parts = append(parts, strings.ToLower(head)+tail)
	}
	return strings.Join(parts, "_")
}
