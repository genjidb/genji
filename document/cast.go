package document

import (
	"encoding/base64"
	"strconv"

	"github.com/genjidb/genji/internal/stringutil"
)

// CastAs casts v as the selected type when possible.
func (v Value) CastAs(t ValueType) (Value, error) {
	if v.Type == t {
		return v, nil
	}

	// Null values always remain null.
	if v.Type == NullValue {
		return v, nil
	}

	switch t {
	case BoolValue:
		return v.CastAsBool()
	case IntegerValue:
		return v.CastAsInteger()
	case DoubleValue:
		return v.CastAsDouble()
	case BlobValue:
		return v.CastAsBlob()
	case TextValue:
		return v.CastAsText()
	case ArrayValue:
		return v.CastAsArray()
	case DocumentValue:
		return v.CastAsDocument()
	}

	return Value{}, stringutil.Errorf("cannot cast %s as %q", v.Type, t)
}

// CastAsBool casts according to the following rules:
// Integer: true if truthy, otherwise false.
// Text: uses strconv.Parsebool to determine the boolean value,
// it fails if the text doesn't contain a valid boolean.
// Any other type is considered an invalid cast.
func (v Value) CastAsBool() (Value, error) {
	switch v.Type {
	case BoolValue:
		return v, nil
	case IntegerValue:
		return NewBoolValue(v.V().(int64) != 0), nil
	case TextValue:
		b, err := strconv.ParseBool(v.V().(string))
		if err != nil {
			return Value{}, stringutil.Errorf(`cannot cast %q as bool: %w`, v.V(), err)
		}
		return NewBoolValue(b), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as bool", v.Type)
}

// CastAsInteger casts according to the following rules:
// Bool: returns 1 if true, 0 if false.
// Double: cuts off the decimal and remaining numbers.
// Text: uses strconv.ParseInt to determine the integer value,
// then casts it to an integer. If it fails uses strconv.ParseFloat
// to determine the double value, then casts it to an integer
// It fails if the text doesn't contain a valid float value.
// Any other type is considered an invalid cast.
func (v Value) CastAsInteger() (Value, error) {
	switch v.Type {
	case IntegerValue:
		return v, nil
	case BoolValue:
		if v.V().(bool) {
			return NewIntegerValue(1), nil
		}
		return NewIntegerValue(0), nil
	case DoubleValue:
		return NewIntegerValue(int64(v.V().(float64))), nil
	case TextValue:
		i, err := strconv.ParseInt(v.V().(string), 10, 64)
		if err != nil {
			intErr := err
			f, err := strconv.ParseFloat(v.V().(string), 64)
			if err != nil {
				return Value{}, stringutil.Errorf(`cannot cast %q as integer: %w`, v.V(), intErr)
			}
			i = int64(f)
		}
		return NewIntegerValue(i), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as integer", v.Type)
}

// CastAsDouble casts according to the following rules:
// Integer: returns a double version of the integer.
// Text: uses strconv.ParseFloat to determine the double value,
// it fails if the text doesn't contain a valid float value.
// Any other type is considered an invalid cast.
func (v Value) CastAsDouble() (Value, error) {
	switch v.Type {
	case DoubleValue:
		return v, nil
	case IntegerValue:
		return NewDoubleValue(float64(v.V().(int64))), nil
	case TextValue:
		f, err := strconv.ParseFloat(v.V().(string), 64)
		if err != nil {
			return Value{}, stringutil.Errorf(`cannot cast %q as double: %w`, v.V(), err)
		}
		return NewDoubleValue(f), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as double", v.Type)
}

// CastAsText returns a JSON representation of v.
// If the representation is a string, it gets unquoted.
func (v Value) CastAsText() (Value, error) {
	if v.Type == TextValue {
		return v, nil
	}

	d, err := v.MarshalJSON()
	if err != nil {
		return Value{}, err
	}

	s := string(d)

	if v.Type == BlobValue {
		s, err = strconv.Unquote(s)
		if err != nil {
			return Value{}, err
		}
	}

	return NewTextValue(s), nil
}

// CastAsBlob casts according to the following rules:
// Text: decodes a base64 string, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsBlob() (Value, error) {
	if v.Type == BlobValue {
		return v, nil
	}

	if v.Type == TextValue {
		b, err := base64.StdEncoding.DecodeString(v.V().(string))
		if err != nil {
			return Value{}, stringutil.Errorf(`cannot cast %q as blob: %w`, v.V(), err)
		}

		return NewBlobValue(b), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as blob", v.Type)
}

// CastAsArray casts according to the following rules:
// Text: decodes a JSON array, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsArray() (Value, error) {
	if v.Type == ArrayValue {
		return v, nil
	}

	if v.Type == TextValue {
		var vb ValueBuffer
		err := vb.UnmarshalJSON([]byte(v.V().(string)))
		if err != nil {
			return Value{}, stringutil.Errorf(`cannot cast %q as array: %w`, v.V(), err)
		}

		return NewArrayValue(&vb), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as array", v.Type)
}

// CastAsDocument casts according to the following rules:
// Text: decodes a JSON object, otherwise fails.
// Any other type is considered an invalid cast.
func (v Value) CastAsDocument() (Value, error) {
	if v.Type == DocumentValue {
		return v, nil
	}

	if v.Type == TextValue {
		var fb FieldBuffer
		err := fb.UnmarshalJSON([]byte(v.V().(string)))
		if err != nil {
			return Value{}, stringutil.Errorf(`cannot cast %q as document: %w`, v.V(), err)
		}

		return NewDocumentValue(&fb), nil
	}

	return Value{}, stringutil.Errorf("cannot cast %s as document", v.Type)
}
