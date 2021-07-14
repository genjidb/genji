package document_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewFromJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected *document.FieldBuffer
		fails    bool
	}{
		{"empty object", "{}", document.NewFieldBuffer(), false},
		{"empty object, missing closing bracket", "{", nil, true},
		{"classic object", `{"a": 1, "b": true, "c": "hello", "d": [1, 2, 3], "e": {"f": "g"}}`,
			document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(1)).
				Add("b", document.NewBoolValue(true)).
				Add("c", document.NewTextValue("hello")).
				Add("d", document.NewArrayValue(document.NewValueBuffer().
					Append(document.NewIntegerValue(1)).
					Append(document.NewIntegerValue(2)).
					Append(document.NewIntegerValue(3)))).
				Add("e", document.NewDocumentValue(document.NewFieldBuffer().Add("f", document.NewTextValue("g")))),
			false},
		{"string values", `{"a": "hello ciao"}`, document.NewFieldBuffer().Add("a", document.NewTextValue("hello ciao")), false},
		{"+integer values", `{"a": 1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(1000)), false},
		{"-integer values", `{"a": -1000}`, document.NewFieldBuffer().Add("a", document.NewIntegerValue(-1000)), false},
		{"+float values", `{"a": 10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(10000000000)), false},
		{"-float values", `{"a": -10000000000.0}`, document.NewFieldBuffer().Add("a", document.NewDoubleValue(-10000000000)), false},
		{"bool values", `{"a": true, "b": false}`, document.NewFieldBuffer().Add("a", document.NewBoolValue(true)).Add("b", document.NewBoolValue(false)), false},
		{"empty arrays", `{"a": []}`, document.NewFieldBuffer().Add("a", document.NewArrayValue(document.NewValueBuffer())), false},
		{"nested arrays", `{"a": [[1,  2]]}`, document.NewFieldBuffer().
			Add("a", document.NewArrayValue(
				document.NewValueBuffer().
					Append(document.NewArrayValue(
						document.NewValueBuffer().
							Append(document.NewIntegerValue(1)).
							Append(document.NewIntegerValue(2)))))), false},
		{"missing comma", `{"a": 1 "b": 2}`, nil, true},
		{"missing closing brackets", `{"a": 1, "b": 2`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := document.NewFromJSON([]byte(test.data))

			fb := document.NewFieldBuffer()
			err := fb.Copy(d)

			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, *test.expected, *fb)
			}
		})
	}

	t.Run("GetByField", func(t *testing.T) {
		d := document.NewFromJSON([]byte(`{"a": 1000}`))

		v, err := d.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(1000), v)

		v, err = d.GetByField("b")
		require.Equal(t, document.ErrFieldNotFound, err)
	})
}

func TestNewFromMap(t *testing.T) {
	m := map[string]interface{}{
		"name":     "foo",
		"age":      10,
		"nilField": nil,
	}

	doc, err := document.NewFromMap(m)
	require.NoError(t, err)

	t.Run("Iterate", func(t *testing.T) {
		counter := make(map[string]int)

		err := doc.Iterate(func(f string, v document.Value) error {
			counter[f]++
			switch f {
			case "name":
				require.Equal(t, m[f], v.V().(string))
			default:
				require.EqualValues(t, m[f], v.V())
			}
			return nil
		})
		require.NoError(t, err)
		require.Len(t, counter, 3)
		require.Equal(t, counter["name"], 1)
		require.Equal(t, counter["age"], 1)
		require.Equal(t, counter["nilField"], 1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := doc.GetByField("name")
		require.NoError(t, err)
		require.Equal(t, document.NewTextValue("foo"), v)

		v, err = doc.GetByField("age")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(10), v)

		v, err = doc.GetByField("nilField")
		require.NoError(t, err)
		require.Equal(t, document.NewNullValue(), v)

		_, err = doc.GetByField("bar")
		require.Equal(t, document.ErrFieldNotFound, err)
	})

	t.Run("Invalid types", func(t *testing.T) {

		// test NewFromMap rejects invalid types
		_, err = document.NewFromMap(8)
		require.Error(t, err, "Expected document.NewFromMap to return an error if the passed parameter is not a map")
		_, err = document.NewFromMap(map[int]float64{2: 4.3})
		require.Error(t, err, "Expected document.NewFromMap to return an error if the passed parameter is not a map with a string key type")
	})
}

func BenchmarkJSONToDocument(b *testing.B) {
	data := []byte(`{"_id":"5f8aefb8e443c6c13afdb305","index":0,"guid":"42c2719e-3371-4b2f-b855-d302a8b7eab0","isActive":true,"balance":"$1,064.79","picture":"http://placehold.it/32x32","age":40,"eyeColor":"blue","name":"Adele Webb","gender":"female","company":"EXTRAGEN","email":"adelewebb@extragen.com","phone":"+1 (964) 409-2397","address":"970 Charles Place, Watrous, Texas, 2522","about":"Amet non do ullamco duis velit sunt esse et cillum nisi mollit ea magna. Tempor ut occaecat proident laborum velit nisi et excepteur exercitation non est labore. Laboris pariatur enim proident et. Qui minim enim et incididunt incididunt adipisicing tempor. Occaecat adipisicing sint ex ut exercitation exercitation voluptate. Laboris adipisicing ut cillum eu cillum est sunt amet Lorem quis pariatur.\r\n","registered":"2016-05-25T10:36:44 -04:00","latitude":64.57112,"longitude":176.136138,"tags":["velit","minim","eiusmod","est","eu","voluptate","deserunt"],"friends":[{"id":0,"name":"Mathis Robertson"},{"id":1,"name":"Cecilia Donaldson"},{"id":2,"name":"Joann Goodwin"}],"greeting":"Hello, Adele Webb! You have 2 unread messages.","favoriteFruit":"apple"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := document.NewFromJSON(data)
		d.Iterate(func(string, document.Value) error {
			return nil
		})
	}
}

func TestNewFromCSV(t *testing.T) {
	headers := []string{"a", "b", "c"}
	columns := []string{"A", "B", "C"}

	d := document.NewFromCSV(headers, columns)
	testutil.RequireDocJSONEq(t, d, `{"a": "A", "b": "B", "c": "C"}`)
}
