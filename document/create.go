// +build !wasm

package document

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/genjidb/genji/internal/stringutil"
)

// NewFromJSON creates a document from raw JSON data.
// The returned document will lazily decode the data.
// If data is not a valid json object, calls to Iterate or GetByField will
// return an error.
func NewFromJSON(data []byte) Document {
	return &jsonEncodedDocument{data}
}

type jsonEncodedDocument struct {
	data []byte
}

func (j jsonEncodedDocument) Iterate(fn func(field string, value Value) error) error {
	return jsonparser.ObjectEach(j.data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return err
		}

		return fn(string(key), v)
	})
}

func (j jsonEncodedDocument) GetByField(field string) (Value, error) {
	v, dt, _, err := jsonparser.Get(j.data, field)
	if dt == jsonparser.NotExist {
		return nil, ErrFieldNotFound
	}
	if err != nil {
		return nil, err
	}

	return parseJSONValue(dt, v)
}

func (j jsonEncodedDocument) MarshalJSON() ([]byte, error) {
	return j.data, nil
}

// NewFromMap creates a document from a map.
// Due to the way maps are designed, iteration order is not guaranteed.
func NewFromMap(m interface{}) (Document, error) {
	M := reflect.ValueOf(m)
	if M.Kind() != reflect.Map || M.Type().Key().Kind() != reflect.String {
		return nil, &ErrUnsupportedType{m, "parameter must be a map with a string key"}
	}
	return mapDocument(M), nil
}

type mapDocument reflect.Value

var _ Document = (*mapDocument)(nil)

func (m mapDocument) Iterate(fn func(field string, value Value) error) error {
	M := reflect.Value(m)
	it := M.MapRange()

	for it.Next() {
		v, err := NewValue(it.Value().Interface())
		if err != nil {
			return err
		}

		err = fn(it.Key().String(), v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m mapDocument) GetByField(field string) (Value, error) {
	M := reflect.Value(m)
	v := M.MapIndex(reflect.ValueOf(field))
	if v == (reflect.Value{}) {
		return nil, ErrFieldNotFound
	}
	return NewValue(v.Interface())
}

// MarshalJSON implements the json.Marshaler interface.
func (m mapDocument) MarshalJSON() ([]byte, error) {
	return jsonDocument{Document: m}.MarshalJSON()
}

// NewFromStruct creates a document from a struct using reflection.
func NewFromStruct(s interface{}) (Document, error) {
	ref := reflect.Indirect(reflect.ValueOf(s))

	if !ref.IsValid() || ref.Kind() != reflect.Struct {
		return nil, errors.New("expected struct or pointer to struct")
	}

	return newFromStruct(ref)
}

func newFromStruct(ref reflect.Value) (Document, error) {
	var fb FieldBuffer
	l := ref.NumField()
	tp := ref.Type()

	for i := 0; i < l; i++ {
		f := ref.Field(i)
		if !f.IsValid() {
			continue
		}

		if f.Kind() == reflect.Ptr {
			if f.IsNil() {
				continue
			}

			f = f.Elem()
		}

		sf := tp.Field(i)

		isUnexported := sf.PkgPath != ""

		if sf.Anonymous {
			if isUnexported && f.Kind() != reflect.Struct {
				continue
			}
			d, err := newFromStruct(f)
			if err != nil {
				return nil, err
			}
			err = d.Iterate(func(field string, value Value) error {
				fb.Add(field, value)
				return nil
			})
			if err != nil {
				return nil, err
			}
			continue
		} else if isUnexported {
			continue
		}

		v, err := NewValue(f.Interface())
		if err != nil {
			return nil, err
		}

		field := strings.ToLower(sf.Name)
		if gtag, ok := sf.Tag.Lookup("genji"); ok {
			if gtag == "-" {
				continue
			}
			field = gtag
		}

		fb.Add(field, v)
	}

	return &fb, nil
}

// NewValue creates a value whose type is infered from x.
func NewValue(x interface{}) (Value, error) {
	// Attempt exact matches first:
	switch v := x.(type) {
	case time.Duration:
		return NewIntegerValue(v.Nanoseconds()), nil
	case time.Time:
		return NewTextValue(v.Format(time.RFC3339Nano)), nil
	case nil:
		return NewNullValue(), nil
	case Document:
		return NewDocumentValue(v), nil
	case Array:
		return NewArrayValue(v), nil
	}

	// Compare by kind to detect type definitions over built-in types.
	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return NewValue(reflect.Indirect(v).Interface())
	case reflect.Bool:
		return NewBoolValue(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewIntegerValue(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		x := v.Uint()
		if x > math.MaxInt64 {
			return nil, stringutil.Errorf("cannot convert unsigned integer struct field to int64: %d out of range", x)
		}
		return NewIntegerValue(int64(x)), nil
	case reflect.Float32, reflect.Float64:
		return NewDoubleValue(v.Float()), nil
	case reflect.String:
		return NewTextValue(v.String()), nil
	case reflect.Interface:
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return NewValue(v.Elem().Interface())
	case reflect.Struct:
		doc, err := NewFromStruct(x)
		if err != nil {
			return nil, err
		}
		return NewDocumentValue(doc), nil
	case reflect.Array:
		return NewArrayValue(&sliceArray{v}), nil
	case reflect.Slice:
		if reflect.TypeOf(v.Interface()).Elem().Kind() == reflect.Uint8 {
			return NewBlobValue(v.Bytes()), nil
		}
		if v.IsNil() {
			return NewNullValue(), nil
		}
		return NewArrayValue(&sliceArray{ref: v}), nil
	case reflect.Map:
		doc, err := NewFromMap(x)
		if err != nil {
			return nil, err
		}
		return NewDocumentValue(doc), nil
	}

	return nil, &ErrUnsupportedType{x, ""}
}

type sliceArray struct {
	ref reflect.Value
}

var _ Array = (*sliceArray)(nil)

func (s sliceArray) Iterate(fn func(i int, v Value) error) error {
	l := s.ref.Len()

	for i := 0; i < l; i++ {
		f := s.ref.Index(i)

		v, err := NewValue(f.Interface())
		if err != nil {
			if err.(*ErrUnsupportedType) != nil {
				continue
			}
			return err
		}

		err = fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s sliceArray) GetByIndex(i int) (Value, error) {
	if i >= s.ref.Len() {
		return nil, ErrFieldNotFound
	}

	v := s.ref.Index(i)
	if !v.IsValid() {
		return nil, ErrFieldNotFound
	}

	return NewValue(v.Interface())
}

// NewFromCSV takes a list of headers and columns and returns a document.
// Each header will be assigned as the key and each corresponding column as a text value.
// The length of headers and columns must be the same.
func NewFromCSV(headers, columns []string) Document {
	fb := NewFieldBuffer()
	for i, h := range headers {
		if i >= len(columns) {
			break
		}

		fb.Add(h, NewTextValue(columns[i]))
	}

	return fb
}
