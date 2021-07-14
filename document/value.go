package document

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math"
	"strconv"

	"github.com/buger/jsonparser"
	"github.com/genjidb/genji/internal/binarysort"
	"github.com/genjidb/genji/internal/stringutil"
)

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func (e *ErrUnsupportedType) Error() string {
	return stringutil.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

// ValueType represents a value type supported by the database.
type ValueType uint8

// List of supported value types.
// These types are separated by family so that when
// new types are introduced we don't need to modify them.
const (
	// denote the absence of type
	AnyType ValueType = 0x0

	NullValue ValueType = 0x80

	BoolValue ValueType = 0x81

	// integer family: 0x90 to 0x9F
	IntegerValue ValueType = 0x90

	// double family: 0xA0 to 0xAF
	DoubleValue ValueType = 0xA0

	// string family: 0xC0 to 0xCF
	TextValue ValueType = 0xC0

	// blob family: 0xD0 to 0xDF
	BlobValue ValueType = 0xD0

	// array family: 0xE0 to 0xEF
	ArrayValue ValueType = 0xE0

	// document family: 0xF0 to 0xFF
	DocumentValue ValueType = 0xF0
)

func (t ValueType) String() string {
	switch t {
	case NullValue:
		return "null"
	case BoolValue:
		return "bool"
	case IntegerValue:
		return "integer"
	case DoubleValue:
		return "double"
	case BlobValue:
		return "blob"
	case TextValue:
		return "text"
	case ArrayValue:
		return "array"
	case DocumentValue:
		return "document"
	}

	return ""
}

// IsNumber returns true if t is either an integer of a float.
func (t ValueType) IsNumber() bool {
	return t == IntegerValue || t == DoubleValue
}

// IsAny returns whether this is type is Any or a real type
func (t ValueType) IsAny() bool {
	return t == AnyType
}

type Val interface {
	Type() ValueType
	v() interface{}
	IsZeroValue() (bool, error)
	Append([]byte) ([]byte, error)
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
}

// A Value stores encoded data alongside its type.
type Value struct {
	Type ValueType
	v    interface{}
}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return Value{
		Type: NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return Value{
		Type: BoolValue,
		v:    x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return Value{
		Type: IntegerValue,
		v:    int64(x),
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return Value{
		Type: DoubleValue,
		v:    x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return Value{
		Type: BlobValue,
		v:    x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return Value{
		Type: TextValue,
		v:    x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return Value{
		Type: ArrayValue,
		v:    a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return Value{
		Type: DocumentValue,
		v:    d,
	}
}

func (v Value) V() interface{} {
	return v.v
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func (v Value) IsTruthy() (bool, error) {
	if v.Type == NullValue {
		return false, nil
	}

	b, err := v.IsZeroValue()
	return !b, err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func (v Value) IsZeroValue() (bool, error) {
	switch v.Type {
	case BoolValue:
		return v.v == false, nil
	case IntegerValue:
		return v.v == int64(0), nil
	case DoubleValue:
		return v.v == float64(0), nil
	case BlobValue:
		return v.v == nil, nil
	case TextValue:
		return v.v == "", nil
	case ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := v.v.(Array).GetByIndex(0)
		if err == ErrValueNotFound {
			return true, nil
		}
		return false, err
	case DocumentValue:
		err := v.v.(Document).Iterate(func(_ string, _ Value) error {
			// We return an error in the first iteration to stop it.
			return errStop
		})
		if err == nil {
			// If err is nil, it means that we didn't iterate,
			// thus the document is empty.
			return true, nil
		}
		if err == errStop {
			// If err is errStop, it means that we iterate
			// at least once, thus the document is not empty.
			return false, nil
		}
		// An unexpecting error occurs, let's return it!
		return false, err
	}

	return false, nil
}

// MarshalJSON implements the json.Marshaler interface.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case NullValue:
		return []byte("null"), nil
	case BoolValue:
		return strconv.AppendBool(nil, v.v.(bool)), nil
	case IntegerValue:
		return strconv.AppendInt(nil, v.v.(int64), 10), nil
	case DoubleValue:
		f := v.v.(float64)
		abs := math.Abs(f)
		fmt := byte('f')
		if abs != 0 {
			if abs < 1e-6 || abs >= 1e21 {
				fmt = 'e'
			}
		}

		// By default the precision is -1 to use the smallest number of digits.
		// See https://pkg.go.dev/strconv#FormatFloat
		prec := -1

		return strconv.AppendFloat(nil, v.v.(float64), fmt, prec, 64), nil
	case TextValue:
		return []byte(strconv.Quote(v.v.(string))), nil
	case BlobValue:
		src := v.v.([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case ArrayValue:
		return jsonArray{v.v.(Array)}.MarshalJSON()
	case DocumentValue:
		return jsonDocument{v.v.(Document)}.MarshalJSON()
	default:
		return nil, stringutil.Errorf("unexpected type: %d", v.Type)
	}
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v Value) String() string {
	switch v.Type {
	case NullValue:
		return "NULL"
	case TextValue:
		return strconv.Quote(v.v.(string))
	case BlobValue:
		return stringutil.Sprintf("%v", v.v)
	}

	d, _ := v.MarshalJSON()
	return string(d)
}

// Append appends to buf a binary representation of v.
// The encoded value doesn't include type information.
func (v Value) Append(buf []byte) ([]byte, error) {
	switch v.Type {
	case BlobValue:
		return append(buf, v.v.([]byte)...), nil
	case TextValue:
		return append(buf, v.v.(string)...), nil
	case BoolValue:
		return binarysort.AppendBool(buf, v.v.(bool)), nil
	case IntegerValue:
		return binarysort.AppendInt64(buf, v.v.(int64)), nil
	case DoubleValue:
		return binarysort.AppendFloat64(buf, v.v.(float64)), nil
	case NullValue:
		return buf, nil
	case ArrayValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendArray(v.v.(Array))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case DocumentValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendDocument(v.v.(Document))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return nil, errors.New("cannot encode type " + v.Type.String() + " as key")
}

// MarshalBinary returns a binary representation of v.
// The encoded value doesn't include type information.
func (v Value) MarshalBinary() ([]byte, error) {
	return v.Append(nil)
}

// Add u to v and return the result.
// Only numeric values and booleans can be added together.
func (v Value) Add(u Value) (res Value, err error) {
	return calculateValues(v, u, '+')
}

// Sub calculates v - u and returns the result.
// Only numeric values and booleans can be calculated together.
func (v Value) Sub(u Value) (res Value, err error) {
	return calculateValues(v, u, '-')
}

// Mul calculates v * u and returns the result.
// Only numeric values and booleans can be calculated together.
func (v Value) Mul(u Value) (res Value, err error) {
	return calculateValues(v, u, '*')
}

// Div calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) Div(u Value) (res Value, err error) {
	return calculateValues(v, u, '/')
}

// Mod calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) Mod(u Value) (res Value, err error) {
	return calculateValues(v, u, '%')
}

// BitwiseAnd calculates v & u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseAnd(u Value) (res Value, err error) {
	return calculateValues(v, u, '&')
}

// BitwiseOr calculates v | u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseOr(u Value) (res Value, err error) {
	return calculateValues(v, u, '|')
}

// BitwiseXor calculates v ^ u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func (v Value) BitwiseXor(u Value) (res Value, err error) {
	return calculateValues(v, u, '^')
}

func calculateValues(a, b Value, operator byte) (res Value, err error) {
	if a.Type == NullValue || b.Type == NullValue {
		return NewNullValue(), nil
	}

	if a.Type == BoolValue || b.Type == BoolValue {
		return NewNullValue(), nil
	}

	if a.Type.IsNumber() && b.Type.IsNumber() {
		if a.Type == DoubleValue || b.Type == DoubleValue {
			return calculateFloats(a, b, operator)
		}

		return calculateIntegers(a, b, operator)
	}

	return NewNullValue(), nil
}

func calculateIntegers(a, b Value, operator byte) (res Value, err error) {
	var xa, xb int64

	ia, err := a.CastAsInteger()
	if err != nil {
		return NewNullValue(), nil
	}
	xa = ia.v.(int64)

	ib, err := b.CastAsInteger()
	if err != nil {
		return NewNullValue(), nil
	}
	xb = ib.v.(int64)

	var xr int64

	switch operator {
	case '-':
		xb = -xb
		fallthrough
	case '+':
		xr = xa + xb
		// if there is an integer overflow
		// convert to float
		if (xr > xa) != (xb > 0) {
			return NewDoubleValue(float64(xa) + float64(xb)), nil
		}
		return NewIntegerValue(xr), nil
	case '*':
		if xa == 0 || xb == 0 {
			return NewIntegerValue(0), nil
		}

		xr = xa * xb
		// if there is no integer overflow
		// return an int, otherwise
		// convert to float
		if (xr < 0) == ((xa < 0) != (xb < 0)) {
			if xr/xb == xa {
				return NewIntegerValue(xr), nil
			}
		}
		return NewDoubleValue(float64(xa) * float64(xb)), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa / xb), nil
	case '%':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewIntegerValue(xa % xb), nil
	case '&':
		return NewIntegerValue(xa & xb), nil
	case '|':
		return NewIntegerValue(xa | xb), nil
	case '^':
		return NewIntegerValue(xa ^ xb), nil
	default:
		panic(stringutil.Sprintf("unknown operator %c", operator))
	}
}

func calculateFloats(a, b Value, operator byte) (res Value, err error) {
	var xa, xb float64

	fa, err := a.CastAsDouble()
	if err != nil {
		return NewNullValue(), nil
	}
	xa = fa.v.(float64)

	fb, err := b.CastAsDouble()
	if err != nil {
		return NewNullValue(), nil
	}
	xb = fb.v.(float64)

	switch operator {
	case '+':
		return NewDoubleValue(xa + xb), nil
	case '-':
		return NewDoubleValue(xa - xb), nil
	case '*':
		return NewDoubleValue(xa * xb), nil
	case '/':
		if xb == 0 {
			return NewNullValue(), nil
		}

		return NewDoubleValue(xa / xb), nil
	case '%':
		mod := math.Mod(xa, xb)

		if math.IsNaN(mod) {
			return NewNullValue(), nil
		}

		return NewDoubleValue(mod), nil
	case '&':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia & ib), nil
	case '|':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia | ib), nil
	case '^':
		ia, ib := int64(xa), int64(xb)
		return NewIntegerValue(ia ^ ib), nil
	default:
		panic(stringutil.Sprintf("unknown operator %c", operator))
	}
}

func parseJSONValue(dataType jsonparser.ValueType, data []byte) (v Value, err error) {
	switch dataType {
	case jsonparser.Null:
		return NewNullValue(), nil
	case jsonparser.Boolean:
		b, err := jsonparser.ParseBoolean(data)
		if err != nil {
			return Value{}, err
		}
		return NewBoolValue(b), nil
	case jsonparser.Number:
		i, err := jsonparser.ParseInt(data)
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := jsonparser.ParseFloat(data)
			if err != nil {
				return Value{}, err
			}

			return NewDoubleValue(f), nil
		}

		return NewIntegerValue(i), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return Value{}, err
		}
		return NewTextValue(s), nil
	case jsonparser.Array:
		buf := NewValueBuffer()
		err := buf.UnmarshalJSON(data)
		if err != nil {
			return Value{}, err
		}

		return NewArrayValue(buf), nil
	case jsonparser.Object:
		buf := NewFieldBuffer()
		err = buf.UnmarshalJSON(data)
		if err != nil {
			return Value{}, err
		}

		return NewDocumentValue(buf), nil
	default:
	}

	return Value{}, nil
}
