package document_test

import (
	"math"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func TestValueString(t *testing.T) {
	tests := []struct {
		name     string
		value    document.Value
		expected string
	}{
		{"null", document.NewNullValue(), "NULL"},
		{"bytes", document.NewBlobValue([]byte("bar")), "[98 97 114]"},
		{"string", document.NewTextValue("bar"), "\"bar\""},
		{"bool", document.NewBoolValue(true), "true"},
		{"int", document.NewIntegerValue(10), "10"},
		{"double", document.NewDoubleValue(10.1), "10.1"},
		{"double with no decimal", document.NewDoubleValue(10), "10"},
		{"big double", document.NewDoubleValue(1e21), "1e+21"},
		{"document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), "{\"a\": 10}"},
		{"array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), "[10]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.value.String())
		})
	}
}

func TestNewValue(t *testing.T) {
	type myBytes []byte
	type myString string
	type myUint uint
	type myUint16 uint16
	type myUint32 uint32
	type myUint64 uint64
	type myInt int
	type myInt8 int8
	type myInt16 int16
	type myInt64 int64
	type myFloat64 float64

	now := time.Now()

	tests := []struct {
		name            string
		value, expected interface{}
	}{
		{"bytes", []byte("bar"), []byte("bar")},
		{"string", "bar", "bar"},
		{"bool", true, true},
		{"uint", uint(10), int64(10)},
		{"uint8", uint8(10), int64(10)},
		{"uint16", uint16(10), int64(10)},
		{"uint32", uint32(10), int64(10)},
		{"uint64", uint64(10), int64(10)},
		{"int", int(10), int64(10)},
		{"int8", int8(10), int64(10)},
		{"int16", int16(10), int64(10)},
		{"int32", int32(10), int64(10)},
		{"int64", int64(10), int64(10)},
		{"float64", 10.1, float64(10.1)},
		{"null", nil, nil},
		{"document", document.NewFieldBuffer().Add("a", document.NewIntegerValue(10)), document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))},
		{"array", document.NewValueBuffer(document.NewIntegerValue(10)), document.NewValueBuffer(document.NewIntegerValue(10))},
		{"time", now, now.Format(time.RFC3339Nano)},
		{"bytes", myBytes("bar"), []byte("bar")},
		{"string", myString("bar"), "bar"},
		{"myUint", myUint(10), int64(10)},
		{"myUint16", myUint16(500), int64(500)},
		{"myUint32", myUint32(90000), int64(90000)},
		{"myUint64", myUint64(100), int64(100)},
		{"myInt", myInt(7), int64(7)},
		{"myInt8", myInt8(3), int64(3)},
		{"myInt16", myInt16(500), int64(500)},
		{"myInt64", myInt64(10), int64(10)},
		{"myFloat64", myFloat64(10.1), float64(10.1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := document.NewValue(test.value)
			require.NoError(t, err)
			require.Equal(t, test.expected, v.V())
		})
	}
}

func TestValueAdd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null+null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null+integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)+bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)+bool(false)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)+integer(-10)", document.NewBoolValue(true), document.NewIntegerValue(-10), document.NewNullValue(), false},
		{"integer(-10)+integer(10)", document.NewIntegerValue(-10), document.NewIntegerValue(10), document.NewIntegerValue(0), false},
		{"integer(120)+integer(120)", document.NewIntegerValue(120), document.NewIntegerValue(120), document.NewIntegerValue(240), false},
		{"integer(120)+float64(120)", document.NewIntegerValue(120), document.NewDoubleValue(120), document.NewDoubleValue(240), false},
		{"integer(120)+float64(120.1)", document.NewIntegerValue(120), document.NewDoubleValue(120.1), document.NewDoubleValue(240.1), false},
		{"int64(max)+integer(10)", document.NewIntegerValue(math.MaxInt64), document.NewIntegerValue(10), document.NewDoubleValue(math.MaxInt64 + 10), false},
		{"int64(min)+integer(-10)", document.NewIntegerValue(math.MinInt64), document.NewIntegerValue(-10), document.NewDoubleValue(math.MinInt64 - 10), false},
		{"integer(120)+text('120')", document.NewIntegerValue(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')+text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document+document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array+array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Add(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueSub(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null-null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null-integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)-bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)-bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"bool(true)-integer(-10)", document.NewBoolValue(true), document.NewIntegerValue(-10), document.NewNullValue(), false},
		{"integer(10)-integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(0), false},
		{"int16(250)-int16(220)", document.NewIntegerValue(250), document.NewIntegerValue(220), document.NewIntegerValue(30), false},
		{"integer(120)-float64(620)", document.NewIntegerValue(120), document.NewDoubleValue(620), document.NewDoubleValue(-500), false},
		{"integer(120)-float64(120.1)", document.NewIntegerValue(120), document.NewDoubleValue(120.1), document.NewDoubleValue(-0.09999999999999432), false},
		{"int64(min)-integer(10)", document.NewIntegerValue(math.MinInt64), document.NewIntegerValue(10), document.NewDoubleValue(math.MinInt64 - 10), false},
		{"int64(max)-integer(-10)", document.NewIntegerValue(math.MaxInt64), document.NewIntegerValue(-10), document.NewDoubleValue(math.MaxInt64 + 10), false},
		{"integer(120)-text('120')", document.NewIntegerValue(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')-text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document-document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array-array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Sub(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMult(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null*null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null*integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)*bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)*bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"bool(true)*integer(-10)", document.NewBoolValue(true), document.NewIntegerValue(-10), document.NewNullValue(), false},
		{"integer(10)*integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(100), false},
		{"integer(10)*integer(80)", document.NewIntegerValue(10), document.NewIntegerValue(80), document.NewIntegerValue(800), false},
		{"integer(10)*float64(80)", document.NewIntegerValue(10), document.NewDoubleValue(80), document.NewDoubleValue(800), false},
		{"int64(max)*int64(max)", document.NewIntegerValue(math.MaxInt64), document.NewIntegerValue(math.MaxInt64), document.NewDoubleValue(math.MaxInt64 * math.MaxInt64), false},
		{"integer(120)*text('120')", document.NewIntegerValue(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')*text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document*document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array*array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Mul(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueDiv(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null/null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null/integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)/bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)/bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"integer(10)/integer(0)", document.NewIntegerValue(10), document.NewIntegerValue(0), document.NewNullValue(), false},
		{"integer(10)/float64(0)", document.NewIntegerValue(10), document.NewDoubleValue(0), document.NewNullValue(), false},
		{"integer(10)/integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(1), false},
		{"integer(10)/integer(8)", document.NewIntegerValue(10), document.NewIntegerValue(8), document.NewIntegerValue(1), false},
		{"integer(10)/float64(8)", document.NewIntegerValue(10), document.NewDoubleValue(8), document.NewDoubleValue(1.25), false},
		{"int64(maxint)/float64(maxint)", document.NewIntegerValue(math.MaxInt64), document.NewDoubleValue(math.MaxInt64), document.NewDoubleValue(1), false},
		{"integer(120)/text('120')", document.NewIntegerValue(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')/text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document/document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array/array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Div(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueMod(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null%null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null%integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)%bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)%bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"integer(10)%integer(0)", document.NewIntegerValue(10), document.NewIntegerValue(0), document.NewNullValue(), false},
		{"integer(10)%float64(0)", document.NewIntegerValue(10), document.NewDoubleValue(0), document.NewNullValue(), false},
		{"integer(10)%integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(0), false},
		{"integer(10)%integer(8)", document.NewIntegerValue(10), document.NewIntegerValue(8), document.NewIntegerValue(2), false},
		{"integer(10)%float64(8)", document.NewIntegerValue(10), document.NewDoubleValue(8), document.NewDoubleValue(2), false},
		{"int64(maxint)%float64(maxint)", document.NewIntegerValue(math.MaxInt64), document.NewDoubleValue(math.MaxInt64), document.NewDoubleValue(0), false},
		{"double(> maxint)%int64(100)", document.NewDoubleValue(math.MaxInt64 + 1000), document.NewIntegerValue(100), document.NewDoubleValue(8), false},
		{"int64(100)%float64(> maxint)", document.NewIntegerValue(100), document.NewDoubleValue(math.MaxInt64 + 1000), document.NewDoubleValue(100), false},
		{"integer(120)%text('120')", document.NewIntegerValue(120), document.NewTextValue("120"), document.NewNullValue(), false},
		{"text('120')%text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document%document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array%array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.Mod(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseAnd(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null&null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null&integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)&bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)&bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"integer(10)&integer(0)", document.NewIntegerValue(10), document.NewIntegerValue(0), document.NewIntegerValue(0), false},
		{"double(10.5)&float64(3.2)", document.NewDoubleValue(10.5), document.NewDoubleValue(3.2), document.NewIntegerValue(2), false},
		{"integer(10)&float64(0)", document.NewIntegerValue(10), document.NewDoubleValue(0), document.NewIntegerValue(0), false},
		{"integer(10)&integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(10), false},
		{"integer(10)&integer(8)", document.NewIntegerValue(10), document.NewIntegerValue(8), document.NewIntegerValue(8), false},
		{"integer(10)&float64(8)", document.NewIntegerValue(10), document.NewDoubleValue(8), document.NewIntegerValue(8), false},
		{"text('120')&text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document&document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array&array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseAnd(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseOr(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null|null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null|integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)|bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)|bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"integer(10)|integer(0)", document.NewIntegerValue(10), document.NewIntegerValue(0), document.NewIntegerValue(10), false},
		{"double(10.5)|float64(3.2)", document.NewDoubleValue(10.5), document.NewDoubleValue(3.2), document.NewIntegerValue(11), false},
		{"integer(10)|float64(0)", document.NewIntegerValue(10), document.NewDoubleValue(0), document.NewIntegerValue(10), false},
		{"integer(10)|integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(10), false},
		{"integer(10)|float64(8)", document.NewIntegerValue(10), document.NewDoubleValue(8), document.NewIntegerValue(10), false},
		{"text('120')|text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document|document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array|array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseOr(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestValueBitwiseXor(t *testing.T) {
	tests := []struct {
		name           string
		v, u, expected document.Value
		fails          bool
	}{
		{"null^null", document.NewNullValue(), document.NewNullValue(), document.NewNullValue(), false},
		{"null^integer(10)", document.NewNullValue(), document.NewIntegerValue(10), document.NewNullValue(), false},
		{"bool(true)^bool(true)", document.NewBoolValue(true), document.NewBoolValue(true), document.NewNullValue(), false},
		{"bool(true)^bool(false)", document.NewBoolValue(true), document.NewBoolValue(false), document.NewNullValue(), false},
		{"integer(10)^integer(0)", document.NewIntegerValue(10), document.NewIntegerValue(0), document.NewIntegerValue(10), false},
		{"double(10.5)^double(3.2)", document.NewDoubleValue(10.5), document.NewDoubleValue(3.2), document.NewIntegerValue(9), false},
		{"integer(10)^double(0)", document.NewIntegerValue(10), document.NewDoubleValue(0), document.NewIntegerValue(10), false},
		{"integer(10)^integer(10)", document.NewIntegerValue(10), document.NewIntegerValue(10), document.NewIntegerValue(0), false},
		{"text('120')^text('120')", document.NewTextValue("120"), document.NewTextValue("120"), document.NewNullValue(), false},
		{"document^document", document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewDocumentValue(document.NewFieldBuffer().Add("a", document.NewIntegerValue(10))), document.NewNullValue(), false},
		{"array^array", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(10))), document.NewNullValue(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.v.BitwiseXor(test.u)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}
