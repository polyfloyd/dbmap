package dbmap

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
)

func init() {
	RegisterMapper(jsonMapper{})
}

var jsonBufPool = &sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

type jsonScanner map[string]interface{}

func (js *jsonScanner) Scan(value interface{}) error {
	var in io.Reader
	switch v := value.(type) {
	case string:
		in = strings.NewReader(v)
	case []byte:
		in = bytes.NewReader(v)
	default:
		return fmt.Errorf("can not decode json from %#v", value)
	}
	return json.NewDecoder(in).Decode(js)
}

func (js jsonScanner) Value() (driver.Value, error) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		jsonBufPool.Put(buf)
	}()

	if err := json.NewEncoder(buf).Encode(js); err != nil {
		return nil, err
	}

	return buf.String(), nil
}

type jsonMapper struct{}

func (jsonMapper) Accepts(fieldType reflect.Type) bool {
	return fieldType.ConvertibleTo(reflect.TypeOf(map[string]interface{}{}))
}

func (jsonMapper) Receive(field reflect.Value) (receiver interface{}) {
	return &jsonScanner{}
}

func (jsonMapper) Copy(target, scanned interface{}) {
	reflect.Indirect(reflect.ValueOf(target)).Set(reflect.ValueOf(*scanned.(*jsonScanner)))
}
