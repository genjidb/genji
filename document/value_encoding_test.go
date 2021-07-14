package document

import (
	"bytes"
	"errors"
	"testing"

	"github.com/genjidb/genji/internal/binarysort"
	"github.com/stretchr/testify/require"
)

func TestValueEncoder(t *testing.T) {
	tests := []struct {
		name string
		v    Value
	}{
		{"null", NewNullValue()},
		{"bool", NewBoolValue(true)},
		{"integer", NewIntegerValue(-10)},
		{"double", NewDoubleValue(-3.14)},
		{"text", NewTextValue("foo")},
		{"blob", NewBlobValue([]byte("bar"))},
		{"array", NewArrayValue(NewValueBuffer(
			NewBoolValue(true),
			NewIntegerValue(55),
			NewDoubleValue(789.58),
			NewArrayValue(NewValueBuffer(
				NewBoolValue(false),
				NewIntegerValue(100),
				NewTextValue("baz"),
			)),
			NewBlobValue([]byte("loo")),
			NewDocumentValue(
				NewFieldBuffer().
					Add("foo1", NewBoolValue(true)).
					Add("foo2", NewIntegerValue(55)).
					Add("foo3", NewArrayValue(NewValueBuffer(
						NewBoolValue(false),
						NewIntegerValue(100),
						NewTextValue("baz"),
					))),
			),
		))},
		{"document", NewDocumentValue(
			NewFieldBuffer().
				Add("foo1", NewBoolValue(true)).
				Add("foo2", NewIntegerValue(55)).
				Add("foo3", NewArrayValue(NewValueBuffer(
					NewBoolValue(false),
					NewIntegerValue(100),
					NewTextValue("baz"),
				))).
				Add("foo4", NewDocumentValue(
					NewFieldBuffer().
						Add("foo1", NewBoolValue(true)).
						Add("foo2", NewIntegerValue(55)).
						Add("foo3", NewArrayValue(NewValueBuffer(
							NewBoolValue(false),
							NewIntegerValue(100),
							NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer

			enc := NewValueEncoder(&buf)
			err := enc.Encode(test.v)
			require.NoError(t, err)

			got, err := decodeValue(buf.Bytes())
			require.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}

func TestValueBinaryMarshaling(t *testing.T) {
	tests := []struct {
		name string
		v    Value
	}{
		{"null", NewNullValue()},
		{"bool", NewBoolValue(true)},
		{"integer", NewIntegerValue(-10)},
		{"double", NewDoubleValue(-3.14)},
		{"text", NewTextValue("foo")},
		{"blob", NewBlobValue([]byte("bar"))},
		{"array", NewArrayValue(NewValueBuffer(
			NewBoolValue(true),
			NewIntegerValue(55),
			NewIntegerValue(56),
			NewIntegerValue(57),
			NewDoubleValue(789.58),
			NewDoubleValue(790.58),
			NewDoubleValue(791.58),
			NewArrayValue(NewValueBuffer(
				NewBoolValue(false),
				NewIntegerValue(100),
				NewTextValue("baz"),
			)),
			NewArrayValue(NewValueBuffer(
				NewBoolValue(true),
				NewIntegerValue(101),
				NewTextValue("bax"),
			)),
			NewBlobValue([]byte("coc")),
			NewBlobValue([]byte("ori")),
			NewBlobValue([]byte("co!")),
			NewDocumentValue(
				NewFieldBuffer().
					Add("foo1", NewBoolValue(true)).
					Add("foo2", NewIntegerValue(55)).
					Add("foo3", NewArrayValue(NewValueBuffer(
						NewBoolValue(false),
						NewIntegerValue(100),
						NewTextValue("baz"),
					))),
			),
		))},
		{"document", NewDocumentValue(
			NewFieldBuffer().
				Add("foo1", NewBoolValue(true)).
				Add("foo2", NewIntegerValue(55)).
				Add("foo3", NewArrayValue(NewValueBuffer(
					NewBoolValue(false),
					NewIntegerValue(100),
					NewTextValue("baz"),
				))).
				Add("foo4", NewDocumentValue(
					NewFieldBuffer().
						Add("foo1", NewBoolValue(true)).
						Add("foo2", NewIntegerValue(55)).
						Add("foo3", NewArrayValue(NewValueBuffer(
							NewBoolValue(false),
							NewIntegerValue(100),
							NewTextValue("baz"),
						))),
				)),
		)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := test.v.MarshalBinary()
			require.NoError(t, err)

			got := Value{Type: test.v.Type}
			err = got.UnmarshalBinary(b)
			require.NoError(t, err)
			require.Equal(t, test.v, got)
		})
	}
}

// UnmarshalBinary decodes data to v. Data must not contain type information,
// instead, v.Type must be set.
func (v *Value) UnmarshalBinary(data []byte) error {
	switch v.Type {
	case NullValue:
	case BlobValue:
		v.v = data
	case TextValue:
		v.v = string(data)
	case BoolValue:
		x, err := binarysort.DecodeBool(data)
		if err != nil {
			return err
		}
		v.v = x
	case IntegerValue:
		x, err := binarysort.DecodeInt64(data)
		if err != nil {
			return err
		}
		v.v = x
	case DoubleValue:
		x, err := binarysort.DecodeFloat64(data)
		if err != nil {
			return err
		}
		v.v = x
	case ArrayValue:
		a, _, err := decodeArray(data)
		if err != nil {
			return err
		}
		v.v = a
	case DocumentValue:
		d, _, err := decodeDocument(data)
		if err != nil {
			return err
		}
		v.v = d
	default:
		return errors.New("unknown type")
	}

	return nil
}

// decodeValue decodes a value encoded with ValueEncoder.
func decodeValue(data []byte) (Value, error) {
	t := ValueType(data[0])
	data = data[1:]

	switch t {
	case NullValue:
		return NewNullValue(), nil
	case BlobValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return Value{}, err
		}
		return NewBlobValue(t), nil
	case TextValue:
		t, err := binarysort.DecodeBase64(data)
		if err != nil {
			return Value{}, err
		}
		return NewTextValue(string(t)), nil
	case BoolValue:
		b, err := binarysort.DecodeBool(data)
		if err != nil {
			return Value{}, err
		}
		return NewBoolValue(b), nil
	case IntegerValue:
		x, err := binarysort.DecodeInt64(data)
		if err != nil {
			return Value{}, err
		}
		return NewIntegerValue(x), nil
	case DoubleValue:
		x, err := binarysort.DecodeFloat64(data)
		if err != nil {
			return Value{}, err
		}
		return NewDoubleValue(x), nil
	case ArrayValue:
		a, _, err := decodeArray(data)
		if err != nil {
			return Value{}, err
		}
		return NewArrayValue(a), nil
	case DocumentValue:
		d, _, err := decodeDocument(data)
		if err != nil {
			return Value{}, err
		}
		return NewDocumentValue(d), nil
	}

	return Value{}, errors.New("unknown type")
}

func decodeValueUntil(data []byte, delim, end byte) (Value, int, error) {
	t := ValueType(data[0])
	i := 1

	switch t {
	case ArrayValue:
		a, n, err := decodeArray(data[i:])
		i += n
		if err != nil {
			return Value{}, i, err
		}
		return NewArrayValue(a), i, nil
	case DocumentValue:
		d, n, err := decodeDocument(data[i:])
		i += n
		if err != nil {
			return Value{}, i, err
		}
		return NewDocumentValue(d), i, nil
	case NullValue:
	case BoolValue:
		i++
	case IntegerValue, DoubleValue:
		if i+8 < len(data) && (data[i+8] == delim || data[i+8] == end) {
			i += 8
		} else {
			return Value{}, 0, errors.New("malformed " + t.String())
		}
	case BlobValue, TextValue:
		for i < len(data) && data[i] != delim && data[i] != end {
			i++
		}
	default:
		return Value{}, 0, errors.New("invalid type character")
	}

	v, err := decodeValue(data[:i])
	return v, i, err
}

func decodeArray(data []byte) (Array, int, error) {
	var vb ValueBuffer

	var readCount int
	for len(data) > 0 && data[0] != ArrayEnd {
		v, i, err := decodeValueUntil(data, ArrayValueDelim, ArrayEnd)
		if err != nil {
			return nil, i, err
		}

		vb.Append(v)

		// skip the delimiter
		if data[i] == ArrayValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the array end character
	readCount++

	return &vb, readCount, nil
}

func decodeDocument(data []byte) (Document, int, error) {
	var fb FieldBuffer

	var readCount int
	for len(data) > 0 && data[0] != documentEnd {
		i := 0

		for i < len(data) && data[i] != documentValueDelim {
			i++
		}

		field, err := binarysort.DecodeBase64(data[:i])
		if err != nil {
			return nil, 0, err
		}

		// skip the delimiter
		i++

		if i >= len(data) {
			return nil, 0, errors.New("invalid end of input")
		}

		readCount += i

		data = data[i:]

		v, i, err := decodeValueUntil(data, documentValueDelim, documentEnd)
		if err != nil {
			return nil, i, err
		}

		fb.Add(string(field), v)

		// skip the delimiter
		if data[i] == documentValueDelim {
			i++
		}

		readCount += i

		data = data[i:]
	}

	// skip the document end character
	readCount++

	return &fb, readCount, nil
}
