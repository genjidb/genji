package msgpack

import (
	"io"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// A Codec is a MessagePack implementation of an encoding.Codec.
type Codec struct{}

// NewCodec creates a MessagePack codec.
func NewCodec() Codec {
	return Codec{}
}

// NewEncoder implements the encoding.Codec interface.
func (c Codec) NewEncoder(w io.Writer) encoding.Encoder {
	return NewEncoder(w)
}

// NewDocument implements the encoding.Codec interface.
func (c Codec) NewDecoder(data []byte) encoding.Decoder {
	return NewEncodedDocument(data)
}

// Encoder encodes Genji documents and values
// in MessagePack.
type Encoder struct {
	enc *msgpack.Encoder
}

// NewEncoder creates an Encoder that writes in the given writer.
func NewEncoder(w io.Writer) *Encoder {
	enc := msgpack.GetEncoder()
	enc.Reset(w)
	enc.UseCompactInts(true)

	return &Encoder{
		enc: enc,
	}
}

// EncodeDocument encodes d as a MessagePack map.
func (e *Encoder) EncodeDocument(d document.Document) error {
	var dlen int
	var err error

	fb, ok := d.(*document.FieldBuffer)
	if ok {
		dlen = fb.Len()
	} else {
		dlen, err = document.Length(d)
		if err != nil {
			return err
		}
	}

	if err := e.enc.EncodeMapLen(dlen); err != nil {
		return err
	}

	return d.Iterate(func(f string, v document.Value) error {
		if err := e.enc.EncodeString(f); err != nil {
			return err
		}

		return e.EncodeValue(v)
	})
}

// EncodeArray encodes a as a MessagePack array.
func (e *Encoder) EncodeArray(a document.Array) error {
	var alen int
	var err error

	vb, ok := a.(*document.ValueBuffer)
	if ok {
		alen = vb.Len()
	} else {
		alen, err = document.ArrayLength(a)
		if err != nil {
			return err
		}
	}

	if err := e.enc.EncodeArrayLen(alen); err != nil {
		return err
	}

	return a.Iterate(func(i int, v document.Value) error {
		return e.EncodeValue(v)
	})
}

// EncodeValue encodes v based on its type.
// - document -> map
// - array -> array
// - NULL -> nil
// - text -> string
// - blob -> bytes
// - bool -> bool
// - int8 -> int8
// - int16 -> int16
// - int32 -> int32
// - int64 -> int64
// - float64 -> float64
func (e *Encoder) EncodeValue(v document.Value) error {
	switch v.Type() {
	case types.DocumentValue:
		return e.EncodeDocument(v.V().(document.Document))
	case types.ArrayValue:
		return e.EncodeArray(v.V().(document.Array))
	case types.NullValue:
		return e.enc.EncodeNil()
	case types.TextValue:
		return e.enc.EncodeString(v.V().(string))
	case types.BlobValue:
		return e.enc.EncodeBytes(v.V().([]byte))
	case types.BoolValue:
		return e.enc.EncodeBool(v.V().(bool))
	case types.IntegerValue:
		return e.enc.EncodeInt(v.V().(int64))
	case types.DoubleValue:
		return e.enc.EncodeFloat64(v.V().(float64))
	}

	return e.enc.Encode(v.V())
}

// Close puts the encoder into the pool for reuse.
func (e *Encoder) Close() {
	msgpack.PutEncoder(e.enc)
}

// Decoder decodes Genji documents and values
// from MessagePack.
type Decoder struct {
	dec *msgpack.Decoder
}

// NewDecoder creates a Decoder that reads from the given reader.
func NewDecoder(r io.Reader) *Decoder {
	dec := msgpack.GetDecoder()
	dec.Reset(r)

	return &Decoder{
		dec: dec,
	}
}

// DecodeValue reads one value from the reader and decodes it.
func (d *Decoder) DecodeValue() (v document.Value, err error) {
	c, err := d.dec.PeekCode()
	if err != nil {
		return
	}

	// decode array
	if (msgpcode.IsFixedArray(c)) || (c == msgpcode.Array16) || (c == msgpcode.Array32) {
		var a document.Array
		a, err = d.DecodeArray()
		if err != nil {
			return
		}

		v = document.NewArrayValue(a)
		return
	}

	// decode document
	if (msgpcode.IsFixedMap(c)) || (c == msgpcode.Map16) || (c == msgpcode.Map32) {
		var doc document.Document
		doc, err = d.DecodeDocument()
		if err != nil {
			return
		}

		v = document.NewDocumentValue(doc)
		return
	}

	// decode string
	if msgpcode.IsString(c) {
		var s string
		s, err = d.dec.DecodeString()
		if err != nil {
			return
		}
		v = document.NewTextValue(s)
		return
	}

	// decode fixnum (the msgpack size optimization to encode low value integers)
	// https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
	if msgpcode.IsFixedNum(c) {
		var data int64
		data, err = d.dec.DecodeInt64()
		if err != nil {
			return
		}

		v = document.NewIntegerValue(data)
		return
	}

	// decode the rest
	switch c {
	case msgpcode.Nil:
		err = d.dec.DecodeNil()
		if err != nil {
			return
		}
		v = document.NewNullValue()
		return
	case msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		var data []byte
		data, err = d.dec.DecodeBytes()
		if err != nil {
			return
		}
		v = document.NewBlobValue(data)
		return
	case msgpcode.True, msgpcode.False:
		var data bool
		data, err = d.dec.DecodeBool()
		if err != nil {
			return
		}
		v = document.NewBoolValue(data)
		return
	case msgpcode.Int8, msgpcode.Int16, msgpcode.Int32, msgpcode.Int64, msgpcode.Uint8, msgpcode.Uint16, msgpcode.Uint32, msgpcode.Uint64:
		var data int64
		data, err = d.dec.DecodeInt64()
		if err != nil {
			return
		}
		v = document.NewIntegerValue(data)
		return
	case msgpcode.Double:
		var data float64
		data, err = d.dec.DecodeFloat64()
		if err != nil {
			return
		}
		v = document.NewDoubleValue(data)
		return
	}

	panic(stringutil.Sprintf("unsupported type %v", c))
}

// DecodeDocument decodes one document from the reader.
// If the document is malformed, it will not return an error.
// However, calls to Iterate or GetByField will fail.
func (d *Decoder) DecodeDocument() (document.Document, error) {
	r, err := d.dec.DecodeRaw()
	if err != nil {
		return nil, err
	}

	return NewEncodedDocument(r), nil
}

// DecodeArray decodes one array from the reader.
// If the array is malformed, this function will not return an error.
// However, calls to Iterate or GetByIndex will fail.
func (d *Decoder) DecodeArray() (document.Array, error) {
	r, err := d.dec.DecodeRaw()
	if err != nil {
		return nil, err
	}

	return EncodedArray(r), nil
}

// Close puts the decoder into the pool for reuse.
func (d *Decoder) Close() {
	msgpack.PutDecoder(d.dec)
}
