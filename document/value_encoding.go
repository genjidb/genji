package document

import (
	"errors"
	"io"

	"github.com/genjidb/genji/internal/binarysort"
)

const (
	// ArrayValueDelim is a separator used when encoding document.Array in
	// binary reprsentation
	ArrayValueDelim = 0x1f
	// ArrayEnd is the final separator used when encoding document.Array in
	// binary reprsentation.
	ArrayEnd           = 0x1e
	documentValueDelim = 0x1c
	documentEnd        = 0x1d
)

// ValueEncoder encodes natural sort-ordered representations of values.
// Type information is encoded alongside each value.
type ValueEncoder struct {
	w io.Writer

	buf []byte
}

// NewValueEncoder creates a ValueEncoder that writes to w.
func NewValueEncoder(w io.Writer) *ValueEncoder {
	return &ValueEncoder{
		w: w,
	}
}

func (ve *ValueEncoder) append(data ...byte) error {
	_, err := ve.w.Write(data)
	return err
}

// Encode v to the writer.
func (ve *ValueEncoder) Encode(v Value) error {
	return ve.appendValue(v)
}

func (ve *ValueEncoder) appendValue(v Value) error {
	err := ve.append(byte(v.Type()))
	if err != nil {
		return err
	}

	switch v.Type() {
	case NullValue:
		return nil
	case ArrayValue:
		return ve.appendArray(v.V().(Array))
	case DocumentValue:
		return ve.appendDocument(v.V().(Document))
	}

	ve.buf = ve.buf[:0]

	switch v.Type() {
	case BlobValue:
		ve.buf, err = binarysort.AppendBase64(ve.buf, v.V().([]byte))
	case TextValue:
		text := v.V().(string)
		ve.buf, err = binarysort.AppendBase64(ve.buf, []byte(text))
	case BoolValue:
		ve.buf, err = binarysort.AppendBool(ve.buf, v.V().(bool)), nil
	case IntegerValue:
		ve.buf = binarysort.AppendInt64(ve.buf, v.V().(int64))
	case DoubleValue:
		ve.buf = binarysort.AppendFloat64(ve.buf, v.V().(float64))
	default:
		return errors.New("cannot encode type " + v.Type().String() + " as key")
	}
	if err != nil {
		return err
	}
	ve.append(ve.buf...)

	return err
}

// appendArray encodes an array into a sort-ordered binary representation.
func (ve *ValueEncoder) appendArray(a Array) error {
	err := a.Iterate(func(i int, value Value) error {
		if i > 0 {
			err := ve.append(ArrayValueDelim)
			if err != nil {
				return err
			}
		}

		return ve.appendValue(value)
	})
	if err != nil {
		return err
	}

	return ve.append(ArrayEnd)
}

// appendDocument encodes a document into a sort-ordered binary representation.
func (ve *ValueEncoder) appendDocument(d Document) error {
	var i int
	err := d.Iterate(func(field string, value Value) error {
		var err error

		if i > 0 {
			err = ve.append(documentValueDelim)
			if err != nil {
				return err
			}
		}

		ve.buf, err = binarysort.AppendBase64(ve.buf[:0], []byte(field))
		if err != nil {
			return err
		}
		err = ve.append(ve.buf...)
		if err != nil {
			return err
		}

		err = ve.append(documentValueDelim)
		if err != nil {
			return err
		}

		err = ve.appendValue(value)
		if err != nil {
			return err
		}

		i++
		return nil
	})
	if err != nil {
		return err
	}

	return ve.append(documentEnd)
}
