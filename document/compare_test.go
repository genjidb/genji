package document_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/stretchr/testify/require"
)

func jsonToInteger(t testing.TB, x string) document.Value {
	var i int64
	err := json.Unmarshal([]byte(x), &i)
	require.NoError(t, err)

	return document.NewIntegerValue(i)
}

func jsonToDouble(t testing.TB, x string) document.Value {
	var f float64
	err := json.Unmarshal([]byte(x), &f)
	require.NoError(t, err)

	return document.NewDoubleValue(f)
}

func jsonToBoolean(t testing.TB, x string) document.Value {
	var b bool
	err := json.Unmarshal([]byte(x), &b)
	require.NoError(t, err)

	return document.NewBoolValue(b)
}

func toText(t testing.TB, x string) document.Value {
	return document.NewTextValue(x)
}

func toBlob(t testing.TB, x string) document.Value {
	return document.NewBlobValue([]byte(x))
}

func jsonToArray(t testing.TB, x string) document.Value {
	var vb document.ValueBuffer
	err := json.Unmarshal([]byte(x), &vb)
	require.NoError(t, err)

	return document.NewArrayValue(&vb)
}

func jsonToDocument(t testing.TB, x string) document.Value {
	var fb document.FieldBuffer
	err := json.Unmarshal([]byte(x), &fb)
	require.NoError(t, err)

	return document.NewDocumentValue(fb)
}

func TestCompare(t *testing.T) {
	tests := []struct {
		op        string
		a, b      string
		ok        bool
		converter func(testing.TB, string) document.Value
	}{
		// bool
		{"=", "true", "false", false, jsonToBoolean},
		{"=", "true", "true", true, jsonToBoolean},
		{"!=", "true", "false", true, jsonToBoolean},
		{"!=", "true", "true", false, jsonToBoolean},
		{">", "true", "false", true, jsonToBoolean},
		{">", "false", "true", false, jsonToBoolean},
		{">", "true", "true", false, jsonToBoolean},
		{">=", "true", "false", true, jsonToBoolean},
		{">=", "false", "true", false, jsonToBoolean},
		{">=", "true", "true", true, jsonToBoolean},
		{"<", "true", "false", false, jsonToBoolean},
		{"<", "false", "true", true, jsonToBoolean},
		{"<", "true", "true", false, jsonToBoolean},
		{"<=", "true", "false", false, jsonToBoolean},
		{"<=", "false", "true", true, jsonToBoolean},
		{"<=", "true", "true", true, jsonToBoolean},

		// integer
		{"=", "2", "1", false, jsonToInteger},
		{"=", "2", "2", true, jsonToInteger},
		{"!=", "2", "1", true, jsonToInteger},
		{"!=", "2", "2", false, jsonToInteger},
		{">", "2", "1", true, jsonToInteger},
		{">", "1", "2", false, jsonToInteger},
		{">", "2", "2", false, jsonToInteger},
		{">=", "2", "1", true, jsonToInteger},
		{">=", "1", "2", false, jsonToInteger},
		{">=", "2", "2", true, jsonToInteger},
		{"<", "2", "1", false, jsonToInteger},
		{"<", "1", "2", true, jsonToInteger},
		{"<", "2", "2", false, jsonToInteger},
		{"<=", "2", "1", false, jsonToInteger},
		{"<=", "1", "2", true, jsonToInteger},
		{"<=", "2", "2", true, jsonToInteger},

		// double
		{"=", "2", "1", false, jsonToDouble},
		{"=", "2", "2", true, jsonToDouble},
		{"!=", "2", "1", true, jsonToDouble},
		{"!=", "2", "2", false, jsonToDouble},
		{">", "2", "1", true, jsonToDouble},
		{">", "1", "2", false, jsonToDouble},
		{">", "2", "2", false, jsonToDouble},
		{">=", "2", "1", true, jsonToDouble},
		{">=", "1", "2", false, jsonToDouble},
		{">=", "2", "2", true, jsonToDouble},
		{"<", "2", "1", false, jsonToDouble},
		{"<", "1", "2", true, jsonToDouble},
		{"<", "2", "2", false, jsonToDouble},
		{"<=", "2", "1", false, jsonToDouble},
		{"<=", "1", "2", true, jsonToDouble},
		{"<=", "2", "2", true, jsonToDouble},

		// text
		{"=", "b", "a", false, toText},
		{"=", "b", "b", true, toText},
		{"!=", "b", "a", true, toText},
		{"!=", "b", "b", false, toText},
		{">", "b", "a", true, toText},
		{">", "a", "b", false, toText},
		{">", "b", "b", false, toText},
		{">=", "b", "a", true, toText},
		{">=", "a", "b", false, toText},
		{">=", "b", "b", true, toText},
		{"<", "b", "a", false, toText},
		{"<", "a", "b", true, toText},
		{"<", "b", "b", false, toText},
		{"<=", "b", "a", false, toText},
		{"<=", "a", "b", true, toText},
		{"<=", "b", "b", true, toText},

		// blob
		{"=", "b", "a", false, toBlob},
		{"=", "b", "b", true, toBlob},
		{"!=", "b", "a", true, toBlob},
		{"!=", "b", "b", false, toBlob},
		{">", "b", "a", true, toBlob},
		{">", "a", "b", false, toBlob},
		{">", "b", "b", false, toBlob},
		{">=", "b", "a", true, toBlob},
		{">=", "a", "b", false, toBlob},
		{">=", "b", "b", true, toBlob},
		{"<", "b", "a", false, toBlob},
		{"<", "a", "b", true, toBlob},
		{"<", "b", "b", false, toBlob},
		{"<=", "b", "a", false, toBlob},
		{"<=", "a", "b", true, toBlob},
		{"<=", "b", "b", true, toBlob},

		// array
		{"=", `[]`, `[]`, true, jsonToArray},
		{"=", `[1]`, `[1]`, true, jsonToArray},
		{"=", `[1]`, `[]`, false, jsonToArray},
		{"=", `[1.0, 2]`, `[1, 2]`, true, jsonToArray},
		{"=", `[1,2,3]`, `[1,2,3]`, true, jsonToArray},
		{"!=", `[1]`, `[5]`, true, jsonToArray},
		{"!=", `[1]`, `[1, 1]`, true, jsonToArray},
		{"!=", `[1,2,3]`, `[1,2,3]`, false, jsonToArray},
		{"!=", `[1]`, `[]`, true, jsonToArray},
		{">", `[2]`, `[1]`, true, jsonToArray},
		{">", `[2]`, `[1, 1000]`, true, jsonToArray},
		{">", `[1]`, `[1, 1000]`, false, jsonToArray},
		{">", `[1, 2]`, `[1, 1000]`, false, jsonToArray},
		{">", `[1, 10]`, `[1, true]`, true, jsonToArray},
		{">", `[1, true]`, `[1, 10]`, false, jsonToArray},
		{">", `[2, 1000]`, `[1]`, true, jsonToArray},
		{">", `[2, 1000]`, `[2]`, true, jsonToArray},
		{">", `[1,2,3]`, `[1,2,3]`, false, jsonToArray},
		{">", `[1,2,3]`, `[]`, true, jsonToArray},
		{">=", `[2]`, `[1]`, true, jsonToArray},
		{">=", `[2]`, `[2]`, true, jsonToArray},
		{">=", `[2]`, `[1, 1000]`, true, jsonToArray},
		{">=", `[1]`, `[1, 1000]`, false, jsonToArray},
		{">=", `[1, 2]`, `[1, 2]`, true, jsonToArray},
		{">=", `[1, 2]`, `[1, 1000]`, false, jsonToArray},
		{">=", `[1, 10]`, `[1, true]`, true, jsonToArray},
		{">=", `[1, true]`, `[1, 10]`, false, jsonToArray},
		{">=", `[2, 1000]`, `[1]`, true, jsonToArray},
		{">=", `[2, 1000]`, `[2]`, true, jsonToArray},
		{">=", `[1,2,3]`, `[1,2,3]`, true, jsonToArray},
		{">=", `[1,2,3]`, `[]`, true, jsonToArray},
		{"<", `[1]`, `[2]`, true, jsonToArray},
		{"<", `[1,2,3]`, `[1,2]`, false, jsonToArray},
		{"<", `[1,2,3]`, `[1,2,3]`, false, jsonToArray},
		{"<", `[1,2]`, `[1,2,3]`, true, jsonToArray},
		{"<", `[1, 1000]`, `[2]`, true, jsonToArray},
		{"<", `[2]`, `[2, 1000]`, true, jsonToArray},
		{"<", `[1,2,3]`, `[]`, false, jsonToArray},
		{"<", `[]`, `[1,2,3]`, true, jsonToArray},
		{"<", `[1, 10]`, `[1, true]`, false, jsonToArray},
		{"<", `[1, true]`, `[1, 10]`, true, jsonToArray},
		{"<=", `[1]`, `[2]`, true, jsonToArray},
		{"<=", `[1, 1000]`, `[2]`, true, jsonToArray},
		{"<=", `[1,2,3]`, `[1,2]`, false, jsonToArray},
		{">=", `[2]`, `[1]`, true, jsonToArray},
		{">=", `[2]`, `[2]`, true, jsonToArray},
		{">=", `[2]`, `[1, 1000]`, true, jsonToArray},
		{">=", `[2, 1000]`, `[1]`, true, jsonToArray},
		{"<=", `[1,2,3]`, `[1,2,3]`, true, jsonToArray},
		{"<=", `[]`, `[]`, true, jsonToArray},
		{"<=", `[]`, `[1,2,3]`, true, jsonToArray},

		// document
		{"=", `{}`, `{}`, true, jsonToDocument},
		{"=", `{"a": 1}`, `{"a": 1}`, true, jsonToDocument},
		{"=", `{"a": 1.0}`, `{"a": 1}`, true, jsonToDocument},
		{"=", `{"a": 1, "b": 2}`, `{"b": 2, "a": 1}`, true, jsonToDocument},
		{"=", `{"a": 1, "b": {"a": 1}}`, `{"b": {"a": 1}, "a": 1}`, true, jsonToDocument},
		{">", `{"a": 2}`, `{"a": 1}`, true, jsonToDocument},
		{">", `{"b": 1}`, `{"a": 1}`, true, jsonToDocument},
		{">", `{"a": 1}`, `{"a": 1}`, false, jsonToDocument},
		{">", `{"a": 1}`, `{"a": true}`, true, jsonToDocument},
		{"<", `{"a": 1}`, `{"a": 2}`, true, jsonToDocument},
		{"<", `{"a": 1}`, `{"b": 1}`, true, jsonToDocument},
		{"<", `{"a": 1}`, `{"a": 1}`, false, jsonToDocument},
		{"<", `{"a": 1}`, `{"a": true}`, false, jsonToDocument},
		{">=", `{"a": 1}`, `{"a": 1}`, true, jsonToDocument},
		{"<=", `{"a": 1}`, `{"a": 1}`, true, jsonToDocument},
	}

	for _, test := range tests {
		a, b := test.converter(t, test.a), test.converter(t, test.b)
		t.Run(fmt.Sprintf("%s/%v%v%v", a.Type().String(), test.a, test.op, test.b), func(t *testing.T) {
			var ok bool
			var err error

			switch test.op {
			case "=":
				ok, err = a.IsEqual(b)
			case "!=":
				ok, err = a.IsNotEqual(b)
			case ">":
				ok, err = a.IsGreaterThan(b)
			case ">=":
				ok, err = a.IsGreaterThanOrEqual(b)
			case "<":
				ok, err = a.IsLesserThan(b)
			case "<=":
				ok, err = a.IsLesserThanOrEqual(b)
			}
			require.NoError(t, err)
			require.Equal(t, test.ok, ok)
		})
	}
}
