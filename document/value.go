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
	"github.com/genjidb/genji/types"
)

// ErrUnsupportedType is used to skip struct or array fields that are not supported.
type ErrUnsupportedType struct {
	Value interface{}
	Msg   string
}

func (e *ErrUnsupportedType) Error() string {
	return stringutil.Sprintf("unsupported type %T. %s", e.Value, e.Msg)
}

type Value interface {
	Type() types.ValueType
	V() interface{}
	// TODO(asdine): Remove the following methods from
	// this interface and use type inference instead.
	MarshalJSON() ([]byte, error)
	MarshalBinary() ([]byte, error)
	String() string
}

// A Value stores encoded data alongside its type.
type value struct {
	tp types.ValueType
	v  interface{}
}

var _ Value = &value{}

// NewNullValue returns a Null value.
func NewNullValue() Value {
	return &value{
		tp: types.NullValue,
	}
}

// NewBoolValue encodes x and returns a value.
func NewBoolValue(x bool) Value {
	return &value{
		tp: types.BoolValue,
		v:  x,
	}
}

// NewIntegerValue encodes x and returns a value whose type depends on the
// magnitude of x.
func NewIntegerValue(x int64) Value {
	return &value{
		tp: types.IntegerValue,
		v:  int64(x),
	}
}

// NewDoubleValue encodes x and returns a value.
func NewDoubleValue(x float64) Value {
	return &value{
		tp: types.DoubleValue,
		v:  x,
	}
}

// NewBlobValue encodes x and returns a value.
func NewBlobValue(x []byte) Value {
	return &value{
		tp: types.BlobValue,
		v:  x,
	}
}

// NewTextValue encodes x and returns a value.
func NewTextValue(x string) Value {
	return &value{
		tp: types.TextValue,
		v:  x,
	}
}

// NewArrayValue returns a value of type Array.
func NewArrayValue(a Array) Value {
	return &value{
		tp: types.ArrayValue,
		v:  a,
	}
}

// NewDocumentValue returns a value of type Document.
func NewDocumentValue(d Document) Value {
	return &value{
		tp: types.DocumentValue,
		v:  d,
	}
}

// NewEmptyValue creates an empty value with the given type.
// V() always returns nil.
func NewEmptyValue(t types.ValueType) Value {
	return &value{
		tp: t,
	}
}

// NewValueWith creates a value with the given type and value.
func NewValueWith(t types.ValueType, v interface{}) Value {
	return &value{
		tp: t,
		v:  v,
	}
}

func (v *value) V() interface{} {
	return v.v
}

func (v *value) Type() types.ValueType {
	return v.tp
}

// IsTruthy returns whether v is not equal to the zero value of its type.
func IsTruthy(v Value) (bool, error) {
	if v.Type() == types.NullValue {
		return false, nil
	}

	b, err := IsZeroValue(v)
	return !b, err
}

// IsZeroValue indicates if the value data is the zero value for the value type.
// This function doesn't perform any allocation.
func IsZeroValue(v Value) (bool, error) {
	switch v.Type() {
	case types.BoolValue:
		return v.V() == false, nil
	case types.IntegerValue:
		return v.V() == int64(0), nil
	case types.DoubleValue:
		return v.V() == float64(0), nil
	case types.BlobValue:
		return v.V() == nil, nil
	case types.TextValue:
		return v.V() == "", nil
	case types.ArrayValue:
		// The zero value of an array is an empty array.
		// Thus, if GetByIndex(0) returns the ErrValueNotFound
		// it means that the array is empty.
		_, err := v.V().(Array).GetByIndex(0)
		if err == ErrValueNotFound {
			return true, nil
		}
		return false, err
	case types.DocumentValue:
		err := v.V().(Document).Iterate(func(_ string, _ Value) error {
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
func (v *value) MarshalJSON() ([]byte, error) {
	switch v.tp {
	case types.NullValue:
		return []byte("null"), nil
	case types.BoolValue:
		return strconv.AppendBool(nil, v.v.(bool)), nil
	case types.IntegerValue:
		return strconv.AppendInt(nil, v.v.(int64), 10), nil
	case types.DoubleValue:
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
	case types.TextValue:
		return []byte(strconv.Quote(v.v.(string))), nil
	case types.BlobValue:
		src := v.v.([]byte)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(src))+2)
		dst[0] = '"'
		dst[len(dst)-1] = '"'
		base64.StdEncoding.Encode(dst[1:], src)
		return dst, nil
	case types.ArrayValue:
		return jsonArray{v.v.(Array)}.MarshalJSON()
	case types.DocumentValue:
		return jsonDocument{v.v.(Document)}.MarshalJSON()
	default:
		return nil, stringutil.Errorf("unexpected type: %d", v.tp)
	}
}

// String returns a string representation of the value. It implements the fmt.Stringer interface.
func (v *value) String() string {
	switch v.tp {
	case types.NullValue:
		return "NULL"
	case types.TextValue:
		return strconv.Quote(v.v.(string))
	case types.BlobValue:
		return stringutil.Sprintf("%v", v.v)
	}

	d, _ := v.MarshalJSON()
	return string(d)
}

// Append appends to buf a binary representation of v.
// The encoded value doesn't include type information.
func (v *value) Append(buf []byte) ([]byte, error) {
	switch v.tp {
	case types.BlobValue:
		return append(buf, v.v.([]byte)...), nil
	case types.TextValue:
		return append(buf, v.v.(string)...), nil
	case types.BoolValue:
		return binarysort.AppendBool(buf, v.v.(bool)), nil
	case types.IntegerValue:
		return binarysort.AppendInt64(buf, v.v.(int64)), nil
	case types.DoubleValue:
		return binarysort.AppendFloat64(buf, v.v.(float64)), nil
	case types.NullValue:
		return buf, nil
	case types.ArrayValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendArray(v.v.(Array))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case types.DocumentValue:
		var buf bytes.Buffer
		err := NewValueEncoder(&buf).appendDocument(v.v.(Document))
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return nil, errors.New("cannot encode type " + v.tp.String() + " as key")
}

// MarshalBinary returns a binary representation of v.
// The encoded value doesn't include type information.
func (v *value) MarshalBinary() ([]byte, error) {
	return v.Append(nil)
}

// Add u to v and return the result.
// Only numeric values and booleans can be added together.
func Add(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '+')
}

// Sub calculates v - u and returns the result.
// Only numeric values and booleans can be calculated together.
func Sub(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '-')
}

// Mul calculates v * u and returns the result.
// Only numeric values and booleans can be calculated together.
func Mul(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '*')
}

// Div calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func Div(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '/')
}

// Mod calculates v / u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func Mod(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '%')
}

// BitwiseAnd calculates v & u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseAnd(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '&')
}

// BitwiseOr calculates v | u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseOr(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '|')
}

// BitwiseXor calculates v ^ u and returns the result.
// Only numeric values and booleans can be calculated together.
// If both v and u are integers, the result will be an integer.
func BitwiseXor(v1, v2 Value) (res Value, err error) {
	return calculateValues(v1, v2, '^')
}

func calculateValues(a, b Value, operator byte) (res Value, err error) {
	if a.Type() == types.NullValue || b.Type() == types.NullValue {
		return NewNullValue(), nil
	}

	if a.Type() == types.BoolValue || b.Type() == types.BoolValue {
		return NewNullValue(), nil
	}

	if a.Type().IsNumber() && b.Type().IsNumber() {
		if a.Type() == types.DoubleValue || b.Type() == types.DoubleValue {
			return calculateFloats(a, b, operator)
		}

		return calculateIntegers(a, b, operator)
	}

	return NewNullValue(), nil
}

func calculateIntegers(a, b Value, operator byte) (res Value, err error) {
	var xa, xb int64

	ia, err := CastAsInteger(a)
	if err != nil {
		return NewNullValue(), nil
	}
	xa = ia.V().(int64)

	ib, err := CastAsInteger(b)
	if err != nil {
		return NewNullValue(), nil
	}
	xb = ib.V().(int64)

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

	fa, err := CastAsDouble(a)
	if err != nil {
		return NewNullValue(), nil
	}
	xa = fa.V().(float64)

	fb, err := CastAsDouble(b)
	if err != nil {
		return NewNullValue(), nil
	}
	xb = fb.V().(float64)

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
			return nil, err
		}
		return NewBoolValue(b), nil
	case jsonparser.Number:
		i, err := jsonparser.ParseInt(data)
		if err != nil {
			// if it's too big to fit in an int64, let's try parsing this as a floating point number
			f, err := jsonparser.ParseFloat(data)
			if err != nil {
				return nil, err
			}

			return NewDoubleValue(f), nil
		}

		return NewIntegerValue(i), nil
	case jsonparser.String:
		s, err := jsonparser.ParseString(data)
		if err != nil {
			return nil, err
		}
		return NewTextValue(s), nil
	case jsonparser.Array:
		buf := NewValueBuffer()
		err := buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return NewArrayValue(buf), nil
	case jsonparser.Object:
		buf := NewFieldBuffer()
		err = buf.UnmarshalJSON(data)
		if err != nil {
			return nil, err
		}

		return NewDocumentValue(buf), nil
	default:
	}

	return nil, nil
}
