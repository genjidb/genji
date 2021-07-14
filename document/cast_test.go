package document

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCastAs(t *testing.T) {
	type test struct {
		v, want Value
		fails   bool
	}

	boolV := NewBoolValue(true)
	integerV := NewIntegerValue(10)
	doubleV := NewDoubleValue(10.5)
	textV := NewTextValue("foo")
	blobV := NewBlobValue([]byte("abc"))
	arrayV := NewArrayValue(NewValueBuffer().
		Append(NewTextValue("bar")).
		Append(integerV))
	docV := NewDocumentValue(NewFieldBuffer().
		Add("a", integerV).
		Add("b", textV))

	check := func(t *testing.T, targetType ValueType, tests []test) {
		for _, test := range tests {
			t.Run(test.v.String(), func(t *testing.T) {
				got, err := CastAs(test.v, targetType)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.want, got)
				}
			})
		}
	}

	t.Run("bool", func(t *testing.T) {
		check(t, BoolValue, []test{
			{boolV, boolV, false},
			{integerV, boolV, false},
			{NewIntegerValue(0), NewBoolValue(false), false},
			{doubleV, nil, true},
			{textV, nil, true},
			{NewTextValue("true"), boolV, false},
			{NewTextValue("false"), NewBoolValue(false), false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("integer", func(t *testing.T) {
		check(t, IntegerValue, []test{
			{boolV, NewIntegerValue(1), false},
			{NewBoolValue(false), NewIntegerValue(0), false},
			{integerV, integerV, false},
			{doubleV, integerV, false},
			{textV, nil, true},
			{NewTextValue("10"), integerV, false},
			{NewTextValue("10.5"), integerV, false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("double", func(t *testing.T) {
		check(t, DoubleValue, []test{
			{boolV, nil, true},
			{integerV, NewDoubleValue(10), false},
			{doubleV, doubleV, false},
			{textV, nil, true},
			{NewTextValue("10"), NewDoubleValue(10), false},
			{NewTextValue("10.5"), doubleV, false},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("text", func(t *testing.T) {
		check(t, TextValue, []test{
			{boolV, NewTextValue("true"), false},
			{integerV, NewTextValue("10"), false},
			{doubleV, NewTextValue("10.5"), false},
			{textV, textV, false},
			{blobV, NewTextValue("YWJj"), false},
			{arrayV, NewTextValue(`["bar", 10]`), false},
			{docV,
				NewTextValue(`{"a": 10, "b": "foo"}`),
				false},
		})
	})

	t.Run("blob", func(t *testing.T) {
		check(t, BlobValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{NewTextValue("YWJj"), blobV, false},
			{NewTextValue("   dww  "), nil, true},
			{blobV, blobV, false},
			{arrayV, nil, true},
			{docV, nil, true},
		})
	})

	t.Run("array", func(t *testing.T) {
		check(t, ArrayValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{NewTextValue(`["bar", 10]`), arrayV, false},
			{NewTextValue("abc"), nil, true},
			{blobV, nil, true},
			{arrayV, arrayV, false},
			{docV, nil, true},
		})
	})

	t.Run("document", func(t *testing.T) {
		check(t, DocumentValue, []test{
			{boolV, nil, true},
			{integerV, nil, true},
			{doubleV, nil, true},
			{NewTextValue(`{"a": 10, "b": "foo"}`), docV, false},
			{NewTextValue("abc"), nil, true},
			{blobV, nil, true},
			{arrayV, nil, true},
			{docV, docV, false},
		})
	})
}
