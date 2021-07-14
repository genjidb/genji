package custom

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/internal/binarysort"
)

// A Codec is a custom implementation of an encoding.Codec.
type Codec struct{}

// NewCodec creates a custom codec.
func NewCodec() Codec {
	return Codec{}
}

// NewEncoder implements the encoding.Codec interface.
func (c Codec) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

// NewDocument implements the encoding.Codec interface.
func (c Codec) NewDecoder(data []byte) encoding.Decoder {
	return &EncodedDocument{data}
}

// Encoder encodes Genji documents and values
// in MessagePack.
type Encoder struct {
	w io.Writer
}

// NewEncoder creates an Encoder that writes in the given writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: w,
	}
}

// EncodeDocument encodes d.
func (e *Encoder) EncodeDocument(d document.Document) error {
	data, err := EncodeDocument(d)
	if err != nil {
		return err
	}

	_, err = e.w.Write(data)
	return err
}

// Close does nothing.
func (e *Encoder) Close() {}

// EncodeDocument takes a document and encodes it using the encoding.Format type.
func EncodeDocument(d document.Document) ([]byte, error) {
	if ec, ok := d.(*EncodedDocument); ok {
		return ec.data, nil
	}

	var format Format

	var offset uint64
	var dataList [][]byte

	err := d.Iterate(func(f string, v document.Value) error {
		data, err := EncodeValue(v)
		if err != nil {
			return err
		}

		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize:   uint64(len(f)),
			NameString: f,
			Type:       uint64(v.Type()),
			Size:       uint64(len(data)),
			Offset:     offset,
		})

		offset += uint64(len(data))
		dataList = append(dataList, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	_, err = format.Header.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	buf.Grow(format.Header.BodySize())

	for _, data := range dataList {
		_, err = buf.Write(data)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeDocument takes a byte slice and returns a lazily decoded document.
// If buf is malformed, an error will be returned when calling one of the document method.
func DecodeDocument(buf []byte) document.Document {
	return &EncodedDocument{buf}
}

// EncodeValue encodes any value to a binary representation.
func EncodeValue(v document.Value) ([]byte, error) {
	switch v.Type() {
	case document.DocumentValue:
		return EncodeDocument(v.V().(document.Document))
	case document.ArrayValue:
		return EncodeArray(v.V().(document.Array))
	case document.BlobValue:
		return v.V().([]byte), nil
	case document.TextValue:
		return []byte(v.V().(string)), nil
	case document.BoolValue:
		return binarysort.AppendBool(nil, v.V().(bool)), nil
	case document.IntegerValue:
		return encodeInt64(v.V().(int64)), nil
	case document.DoubleValue:
		return binarysort.AppendFloat64(nil, v.V().(float64)), nil
	case document.NullValue:
		return nil, nil
	}

	return nil, errors.New("unknown type")
}

func encodeInt64(x int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, x)
	return buf[:n]
}

// An EncodedDocument implements the document.Document interface on top of an encoded representation of a
// document.
// It is useful to avoid decoding the entire document when only a few fields are needed.
type EncodedDocument struct {
	data []byte
}

// GetByField decodes the selected field.
func (e *EncodedDocument) GetByField(field string) (document.Value, error) {
	return decodeValueFromDocument(e.data, field)
}

func (e *EncodedDocument) Reset(data []byte) { e.data = data }

// Iterate decodes each fields one by one and passes them to fn until the end of the document
// or until fn returns an error.
func (e *EncodedDocument) Iterate(fn func(field string, value document.Value) error) error {
	var format Format
	err := format.Decode(e.data)
	if err != nil {
		return err
	}

	for _, fh := range format.Header.FieldHeaders {
		v, err := DecodeValue(document.ValueType(fh.Type), format.Body[fh.Offset:fh.Offset+fh.Size])
		if err != nil {
			return err
		}

		err = fn(string(fh.Name), v)
		if err != nil {
			return err
		}
	}

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (e *EncodedDocument) MarshalJSON() ([]byte, error) {
	return document.MarshalJSON(e)
}

// An EncodedArray implements the document.Array interface on top of an encoded representation of an
// array.
// It is useful to avoid decoding the entire array when only a few values are needed.
type EncodedArray []byte

// Iterate goes through all the values of the array and calls the given function by passing each one of them.
// If the given function returns an error, the iteration stops.
func (e EncodedArray) Iterate(fn func(i int, value document.Value) error) error {
	var format Format
	err := format.Decode(e)
	if err != nil {
		return err
	}

	for _, fh := range format.Header.FieldHeaders {
		v, err := DecodeValue(document.ValueType(fh.Type), format.Body[fh.Offset:fh.Offset+fh.Size])
		if err != nil {
			return err
		}

		i, _ := binary.Varint(fh.Name)
		err = fn(int(i), v)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetByIndex returns a value by index of the array.
func (e EncodedArray) GetByIndex(i int) (document.Value, error) {
	v, err := decodeValueFromDocument(e, string(encodeInt64(int64(i))))
	if err == document.ErrFieldNotFound {
		return v, document.ErrValueNotFound
	}

	return v, err
}

// MarshalJSON implements the json.Marshaler interface.
func (e EncodedArray) MarshalJSON() ([]byte, error) {
	return document.MarshalJSONArray(e)
}

func decodeValueFromDocument(data []byte, field string) (document.Value, error) {
	hsize, n := binary.Uvarint(data)
	if n <= 0 {
		return document.Value{}, errors.New("cannot decode data")
	}

	hdata := data[n : n+int(hsize)]
	body := data[n+len(hdata):]

	// skip number of fields
	_, n = binary.Uvarint(hdata)
	if n <= 0 {
		return document.Value{}, errors.New("cannot decode data")
	}
	hdata = hdata[n:]

	var fh FieldHeader
	for len(hdata) > 0 {
		n, err := fh.Decode(hdata)
		if err != nil {
			return document.Value{}, err
		}
		hdata = hdata[n:]

		if field == string(fh.Name) {
			return DecodeValue(document.ValueType(fh.Type), body[fh.Offset:fh.Offset+fh.Size])
		}
	}

	return document.Value{}, document.ErrFieldNotFound
}

// EncodeArray encodes a into its binary representation.
func EncodeArray(a document.Array) ([]byte, error) {
	var format Format

	var offset uint64
	var dataList [][]byte

	err := a.Iterate(func(i int, v document.Value) error {
		data, err := EncodeValue(v)
		if err != nil {
			return err
		}

		index := encodeInt64(int64(i))

		format.Header.FieldHeaders = append(format.Header.FieldHeaders, FieldHeader{
			NameSize: uint64(len(index)),
			Name:     index,
			Type:     uint64(v.Type()),
			Size:     uint64(len(data)),
			Offset:   offset,
		})

		offset += uint64(len(data))
		dataList = append(dataList, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	_, err = format.Header.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	buf.Grow(format.Header.BodySize())

	for _, data := range dataList {
		_, err = buf.Write(data)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeArray takes a byte slice and returns a lazily decoded array.
// If buf is malformed, an error will be returned when calling one of the array method.
func DecodeArray(buf []byte) document.Array {
	return EncodedArray(buf)
}

// DecodeValue takes some encoded data and decodes it to the target type t.
func DecodeValue(t document.ValueType, data []byte) (document.Value, error) {
	switch t {
	case document.DocumentValue:
		return document.NewDocumentValue(&EncodedDocument{data}), nil
	case document.ArrayValue:
		return document.NewArrayValue(EncodedArray(data)), nil
	case document.BlobValue:
		return document.NewBlobValue(data), nil
	case document.TextValue:
		return document.NewTextValue(string(data)), nil
	case document.BoolValue:
		x, err := binarysort.DecodeBool(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewBoolValue(x), nil
	case document.IntegerValue:
		x, _ := binary.Varint(data)
		return document.NewIntegerValue(x), nil
	case document.DoubleValue:
		x, err := binarysort.DecodeFloat64(data)
		if err != nil {
			return document.Value{}, err
		}
		return document.NewDoubleValue(x), nil
	case document.NullValue:
		return document.NewNullValue(), nil
	}

	return document.Value{}, errors.New("unknown type")
}
