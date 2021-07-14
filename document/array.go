package document

import (
	"bytes"
	"errors"
	"sort"

	"github.com/buger/jsonparser"
)

// ErrValueNotFound must be returned by Array implementations, when calling the GetByIndex method and
// the index wasn't found in the array.
var (
	ErrValueNotFound = errors.New("value not found")
)

// An Array contains a set of values.
type Array interface {
	// Iterate goes through all the values of the array and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(i int, value Value) error) error
	// GetByIndex returns a value by index of the array.
	GetByIndex(i int) (Value, error)
}

// ArrayLength returns the length of an array.
func ArrayLength(a Array) (int, error) {
	if vb, ok := a.(*ValueBuffer); ok {
		return len(vb.Values), nil
	}

	var len int
	err := a.Iterate(func(_ int, _ Value) error {
		len++
		return nil
	})
	return len, err
}

var errStop = errors.New("stop")

// ArrayContains iterates over a and returns whether v is equal to one of its values.
func ArrayContains(a Array, v Value) (bool, error) {
	var found bool

	err := a.Iterate(func(i int, vv Value) error {
		ok, err := vv.IsEqual(v)
		if err != nil {
			return err
		}
		if ok {
			found = true
			return errStop
		}

		return nil
	})

	if err != nil && err != errStop {
		return false, err
	}

	return found, nil
}

// ValueBuffer is an array that holds values in memory.
type ValueBuffer struct {
	Values []Value
}

// NewValueBuffer creates a buffer of values.
func NewValueBuffer(values ...Value) *ValueBuffer {
	return &ValueBuffer{Values: values}
}

// Iterate over all the values of the buffer. It implements the Array interface.
func (vb *ValueBuffer) Iterate(fn func(i int, value Value) error) error {
	for i, v := range vb.Values {
		err := fn(i, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value set at the given index. If the index is out of range it returns an error.
func (vb *ValueBuffer) GetByIndex(i int) (Value, error) {
	if i >= len(vb.Values) {
		return Value{}, ErrFieldNotFound
	}

	return vb.Values[i], nil
}

// Len returns the length the of array
func (vb *ValueBuffer) Len() int {
	if vb == nil {
		return 0
	}

	return len(vb.Values)
}

// Append a value to the buffer and return a new buffer.
func (vb *ValueBuffer) Append(v Value) *ValueBuffer {
	vb.Values = append(vb.Values, v)
	return vb
}

// ScanArray copies all the values of a to the buffer.
func (vb *ValueBuffer) ScanArray(a Array) error {
	return a.Iterate(func(i int, v Value) error {
		vb.Values = append(vb.Values, v)
		return nil
	})
}

// Copy deep copies all the values from the given array.
// If a value is a document or an array, it will be stored as a *FieldBuffer or *ValueBuffer respectively.
func (vb *ValueBuffer) Copy(a Array) error {
	err := vb.ScanArray(a)
	if err != nil {
		return err
	}

	if len(vb.Values) == 0 {
		return nil
	}

	for i, v := range vb.Values {
		switch v.Type {
		case DocumentValue:
			var buf FieldBuffer
			err = buf.Copy(v.V().(Document))
			if err != nil {
				return err
			}

			err = vb.Replace(i, NewDocumentValue(&buf))
			if err != nil {
				return err
			}
		case ArrayValue:
			var buf ValueBuffer
			err = buf.Copy(v.V().(Array))
			if err != nil {
				return err
			}

			err = vb.Replace(i, NewArrayValue(&buf))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Apply a function to all the values of the buffer.
func (vb *ValueBuffer) Apply(fn func(p Path, v Value) (Value, error)) error {
	path := Path{PathFragment{}}

	for i, v := range vb.Values {
		path[0].ArrayIndex = i

		switch v.Type {
		case DocumentValue:
			buf, ok := v.V().(*FieldBuffer)
			if !ok {
				buf = NewFieldBuffer()
				err := buf.Copy(v.V().(Document))
				if err != nil {
					return err
				}
			}

			err := buf.Apply(func(p Path, v Value) (Value, error) {
				return fn(append(path, p...), v)
			})
			if err != nil {
				return err
			}
			vb.Values[i] = NewDocumentValue(buf)
		case ArrayValue:
			buf, ok := v.V().(*ValueBuffer)
			if !ok {
				buf = NewValueBuffer()
				err := buf.Copy(v.V().(Array))
				if err != nil {
					return err
				}
			}

			err := buf.Apply(func(p Path, v Value) (Value, error) {
				return fn(append(path, p...), v)
			})
			if err != nil {
				return err
			}
			vb.Values[i] = NewArrayValue(buf)
		default:
			var err error
			v, err = fn(path, v)
			if err != nil {
				return err
			}
			vb.Values[i] = v
		}
	}

	return nil
}

// Replace the value of the index by v.
func (vb *ValueBuffer) Replace(index int, v Value) error {
	if len(vb.Values) <= index {
		return ErrFieldNotFound
	}

	vb.Values[index] = v
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (vb ValueBuffer) MarshalJSON() ([]byte, error) {
	return jsonArray{Array: &vb}.MarshalJSON()
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (vb *ValueBuffer) UnmarshalJSON(data []byte) error {
	var err error
	_, perr := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, _ error) {
		v, err := parseJSONValue(dataType, value)
		if err != nil {
			return
		}

		vb.Values = append(vb.Values, v)
	})
	if err != nil {
		return err
	}
	if perr != nil {
		return perr
	}

	return nil
}

func (vb *ValueBuffer) Types() []ValueType {
	types := make([]ValueType, len(vb.Values))

	for i, v := range vb.Values {
		types[i] = v.Type
	}

	return types
}

// IsEqual compares two ValueBuffer and returns true if and only if
// both each values and types are respectively equal.
func (vb *ValueBuffer) IsEqual(other *ValueBuffer) bool {
	if vb.Len() != other.Len() {
		return false
	}

	// empty buffers are always equal eh
	if vb.Len() == 0 && other.Len() == 0 {
		return true
	}

	otherTypes := other.Types()
	types := vb.Types()

	for i, typ := range types {
		if typ != otherTypes[i] {
			return false
		}
	}

	for i, v := range vb.Values {
		if eq, err := v.IsEqual(other.Values[i]); err != nil || !eq {
			return false
		}
	}

	return true
}

type sortableArray struct {
	vb  *ValueBuffer
	err error
}

func (a sortableArray) Len() int {
	return len(a.vb.Values)
}

func (a *sortableArray) Swap(i, j int) {
	a.vb.Values[i], a.vb.Values[j] = a.vb.Values[j], a.vb.Values[i]
}

func (a *sortableArray) Less(i, j int) (ok bool) {
	it, jt := a.vb.Values[i].Type, a.vb.Values[j].Type
	if it == jt || (it.IsNumber() && jt.IsNumber()) {
		ok, a.err = a.vb.Values[i].IsLesserThan(a.vb.Values[j])
		return
	}

	return it < jt
}

// SortArray creates a new sorted array.
// Types are sorted in the following ascending order:
//   - NULL
//   - Booleans
//   - Numbers
//   - Text
//   - Blob
//   - Arrays
//   - Documents
// It doesn't sort nested arrays.
func SortArray(a Array) (*ValueBuffer, error) {
	var s sortableArray
	vb, ok := a.(*ValueBuffer)
	if !ok {
		vb := NewValueBuffer()
		err := vb.Copy(a)
		if err != nil {
			return nil, err
		}
	}
	s.vb = vb

	sort.Sort(&s)

	if s.err != nil {
		return nil, s.err
	}

	return vb, nil
}

type jsonArray struct {
	Array
}

func (j jsonArray) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('[')
	var notFirst bool
	err := j.Array.Iterate(func(i int, v Value) error {
		if notFirst {
			buf.WriteString(", ")
		}
		notFirst = true

		data, err := v.MarshalJSON()
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}
