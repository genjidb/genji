package document_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

var _ document.Document = new(document.FieldBuffer)

func parsePath(t testing.TB, p string) document.Path {
	path, err := parser.ParsePath(p)
	require.NoError(t, err)
	return path
}

func TestFieldBuffer(t *testing.T) {
	var buf document.FieldBuffer
	buf.Add("a", document.NewIntegerValue(10))
	buf.Add("b", document.NewTextValue("hello"))

	t.Run("Iterate", func(t *testing.T) {
		var i int
		err := buf.Iterate(func(f string, v document.Value) error {
			switch i {
			case 0:
				require.Equal(t, "a", f)
				require.Equal(t, document.NewIntegerValue(10), v)
			case 1:
				require.Equal(t, "b", f)
				require.Equal(t, document.NewTextValue("hello"), v)
			}
			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Add", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewIntegerValue(10))
		buf.Add("b", document.NewTextValue("hello"))

		c := document.NewBoolValue(true)
		buf.Add("c", c)
		require.Equal(t, 3, buf.Len())
	})

	t.Run("ScanDocument", func(t *testing.T) {
		var buf1, buf2 document.FieldBuffer

		buf1.Add("a", document.NewIntegerValue(10))
		buf1.Add("b", document.NewTextValue("hello"))

		buf2.Add("a", document.NewIntegerValue(20))
		buf2.Add("b", document.NewTextValue("bye"))
		buf2.Add("c", document.NewBoolValue(true))

		err := buf1.ScanDocument(buf2)
		require.NoError(t, err)

		var buf document.FieldBuffer
		buf.Add("a", document.NewIntegerValue(10))
		buf.Add("b", document.NewTextValue("hello"))
		buf.Add("a", document.NewIntegerValue(20))
		buf.Add("b", document.NewTextValue("bye"))
		buf.Add("c", document.NewBoolValue(true))
		require.Equal(t, buf, buf1)
	})

	t.Run("GetByField", func(t *testing.T) {
		v, err := buf.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(10), v)

		v, err = buf.GetByField("not existing")
		require.Equal(t, document.ErrFieldNotFound, err)
		require.Zero(t, v)
	})

	t.Run("Fields", func(t *testing.T) {
		require.Equal(t, []string{}, document.NewFieldBuffer().Fields())
		require.Equal(t, []string{"a", "b"}, buf.Fields())
	})

	t.Run("Set", func(t *testing.T) {
		tests := []struct {
			name  string
			data  string
			path  string
			value document.Value
			want  string
			fails bool
		}{
			{"root", `{}`, `a`, document.NewIntegerValue(1), `{"a": 1}`, false},
			{"add field", `{"a": {"b": [1, 2, 3]}}`, `c`, document.NewTextValue("foo"), `{"a": {"b": [1, 2, 3]}, "c": "foo"}`, false},
			{"nested doc", `{"a": "foo"}`, `a`, document.NewDocumentValue(document.NewFieldBuffer().
				Add("b", document.NewArrayValue(document.NewValueBuffer().
					Append(document.NewIntegerValue(1)).
					Append(document.NewIntegerValue(2)).
					Append(document.NewIntegerValue(3))))), `{"a": {"b": [1, 2, 3]}}`, false},
			{"nested doc", `{"a": {"b": [1, 2, 3]}}`, `a.b`, document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntegerValue(1)).
				Append(document.NewIntegerValue(2)).
				Append(document.NewIntegerValue(3))), `{"a": {"b": [1, 2, 3]}}`, false},
			{"nested array", `{"a": {"b": [1, 2, 3]}}`, `a.b[1]`, document.NewIntegerValue(1), `{"a": {"b": [1, 1, 3]}}`, false},
			{"nested array multiple indexes", `{"a": {"b": [1, 2, [1, 2, {"c": "foo"}]]}}`, `a.b[2][2].c`, document.NewTextValue("bar"), `{"a": {"b": [1, 2, [1, 2, {"c": "bar"}]]}}`, false},
			{"number field", `{"a": {"0": [1, 2, 3]}}`, "a.`0`[0]", document.NewIntegerValue(6), `{"a": {"0": [6, 2, 3]}}`, false},
			{"document in array", `{"a": [{"b":"foo"}, 2, 3]}`, `a[0].b`, document.NewTextValue("bar"), `{"a": [{"b": "bar"}, 2, 3]}`, false},
			// with errors or request ignored doc unchanged
			{"field not found", `{"a": {"b": [1, 2, 3]}}`, `a.b.c`, document.NewIntegerValue(1), `{"a": {"b": [1, 2, 3]}}`, false},
			{"unknown path", `{"a": {"b": [1, 2, 3]}}`, `a.e.f`, document.NewIntegerValue(1), ``, true},
			{"index out of range", `{"a": {"b": [1, 2, 3]}}`, `a.b[1000]`, document.NewIntegerValue(1), ``, true},
			{"document not array", `{"a": {"b": "foo"}}`, `a[0].b`, document.NewTextValue("bar"), ``, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var fb document.FieldBuffer

				d := document.NewFromJSON([]byte(tt.data))
				err := fb.Copy(d)
				require.NoError(t, err)
				p, err := parser.ParsePath(tt.path)
				require.NoError(t, err)
				err = fb.Set(p, tt.value)
				if tt.fails {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				data, err := document.MarshalJSON(fb)
				require.NoError(t, err)
				require.Equal(t, tt.want, string(data))
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tests := []struct {
			document   string
			deletePath string
			expected   string
			fails      bool
		}{
			{`{"a": 10, "b": "hello"}`, "a", `{"b": "hello"}`, false},
			{`{"a": 10, "b": "hello"}`, "c", ``, true},
			{`{"a": [1], "b": "hello"}`, "a[0]", `{"a": [], "b": "hello"}`, false},
			{`{"a": [1, 2], "b": "hello"}`, "a[0]", `{"a": [2], "b": "hello"}`, false},
			{`{"a": [1, 2], "b": "hello"}`, "a[5]", ``, true},
			{`{"a": [1, {"c": [1]}], "b": "hello"}`, "a[1].c", `{"a": [1, {}], "b": "hello"}`, false},
			{`{"a": [1, {"c": [1]}], "b": "hello"}`, "a[1].d", ``, true},
		}

		for _, test := range tests {
			t.Run(test.document, func(t *testing.T) {
				var buf document.FieldBuffer
				err := json.Unmarshal([]byte(test.document), &buf)
				require.NoError(t, err)

				path := parsePath(t, test.deletePath)

				err = buf.Delete(path)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					got, err := json.Marshal(&buf)
					require.NoError(t, err)
					require.JSONEq(t, test.expected, string(got))
				}
			})
		}
	})

	t.Run("Replace", func(t *testing.T) {
		var buf document.FieldBuffer
		buf.Add("a", document.NewIntegerValue(10))
		buf.Add("b", document.NewTextValue("hello"))

		err := buf.Replace("a", document.NewBoolValue(true))
		require.NoError(t, err)
		v, err := buf.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewBoolValue(true), v)
		err = buf.Replace("d", document.NewIntegerValue(11))
		require.Error(t, err)
	})

	t.Run("Apply", func(t *testing.T) {
		d := document.NewFromJSON([]byte(`{
			"a": "b",
			"c": ["d", "e"],
			"f": {"g": "h"}
		}`))

		buf := document.NewFieldBuffer()
		err := buf.Copy(d)
		require.NoError(t, err)

		err = buf.Apply(func(p document.Path, v document.Value) (document.Value, error) {
			if v.Type() == document.ArrayValue || v.Type() == document.DocumentValue {
				return v, nil
			}

			return document.NewIntegerValue(1), nil
		})
		require.NoError(t, err)

		got, err := json.Marshal(buf)
		require.NoError(t, err)
		require.JSONEq(t, `{"a": 1, "c": [1, 1], "f": {"g": 1}}`, string(got))
	})

	t.Run("UnmarshalJSON", func(t *testing.T) {
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
				var buf document.FieldBuffer

				err := json.Unmarshal([]byte(test.data), &buf)
				if test.fails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, *test.expected, buf)
				}
			})
		}
	})
}

func TestNewFromStruct(t *testing.T) {
	type group struct {
		Ig int
	}

	type user struct {
		A []byte
		B string
		C bool
		D uint `genji:"la-reponse-d"`
		E uint8
		F uint16
		G uint32
		H uint64
		I int
		J int8
		K int16
		L int32
		M int64
		N float64
		// structs must be considered as documents
		O group

		// nil pointers must be skipped
		// otherwise they must be dereferenced
		P *int
		Q *int

		// struct pointers should be considered as documents
		// if there are nil though, they must be skipped
		R *group
		S *group

		T  []int
		U  []int
		V  []*int
		W  []user
		X  []interface{}
		Y  [3]int
		Z  interface{}
		ZZ interface{}

		AA int `genji:"-"` // ignored

		*group

		BB time.Time // some has special encoding as Document

		// unexported fields should be ignored
		t int
	}

	u := user{
		A:  []byte("foo"),
		B:  "bar",
		C:  true,
		D:  1,
		E:  2,
		F:  3,
		G:  4,
		H:  5,
		I:  6,
		J:  7,
		K:  8,
		L:  9,
		M:  10,
		N:  11.12,
		Z:  26,
		AA: 27,
		group: &group{
			Ig: 100,
		},
		BB: time.Date(2020, 11, 15, 16, 37, 10, 20, time.UTC),
		t:  99,
	}

	q := 5
	u.Q = &q
	u.R = new(group)
	u.T = []int{1, 2, 3}
	u.V = []*int{&q}
	u.W = []user{u}
	u.X = []interface{}{1, "foo"}

	t.Run("Iterate", func(t *testing.T) {
		doc, err := document.NewFromStruct(u)
		require.NoError(t, err)

		var counter int

		err = doc.Iterate(func(f string, v document.Value) error {
			switch counter {
			case 0:
				require.Equal(t, u.A, v.V().([]byte))
			case 1:
				require.Equal(t, u.B, v.V().(string))
			case 2:
				require.Equal(t, u.C, v.V().(bool))
			case 3:
				require.Equal(t, "la-reponse-d", f)
				require.EqualValues(t, u.D, v.V().(int64))
			case 4:
				require.EqualValues(t, u.E, v.V().(int64))
			case 5:
				require.EqualValues(t, u.F, v.V().(int64))
			case 6:
				require.EqualValues(t, u.G, v.V().(int64))
			case 7:
				require.EqualValues(t, u.H, v.V().(int64))
			case 8:
				require.EqualValues(t, u.I, v.V().(int64))
			case 9:
				require.EqualValues(t, u.J, v.V().(int64))
			case 10:
				require.EqualValues(t, u.K, v.V().(int64))
			case 11:
				require.EqualValues(t, u.L, v.V().(int64))
			case 12:
				require.EqualValues(t, u.M, v.V().(int64))
			case 13:
				require.Equal(t, u.N, v.V().(float64))
			case 14:
				require.EqualValues(t, document.DocumentValue, v.Type())
			case 15:
				require.EqualValues(t, *u.Q, v.V().(int64))
			case 16:
				require.EqualValues(t, document.DocumentValue, v.Type())
			case 17:
				require.EqualValues(t, document.ArrayValue, v.Type())
			case 18:
				require.EqualValues(t, document.NullValue, v.Type())
			case 19:
				require.EqualValues(t, document.ArrayValue, v.Type())
			case 20:
				require.EqualValues(t, document.ArrayValue, v.Type())
			case 21:
				require.EqualValues(t, document.ArrayValue, v.Type())
			case 22:
				require.EqualValues(t, document.ArrayValue, v.Type())
			case 23:
				require.EqualValues(t, u.Z, v.V().(int64))
			case 24:
				require.EqualValues(t, document.NullValue, v.Type())
			case 25:
				require.EqualValues(t, document.IntegerValue, v.Type())
			case 26:
				require.EqualValues(t, document.TextValue, v.Type())
			default:
				require.FailNowf(t, "", "unknown field %q", f)
			}

			counter++

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 27, counter)
	})

	t.Run("GetByField", func(t *testing.T) {
		doc, err := document.NewFromStruct(u)
		require.NoError(t, err)

		v, err := doc.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, u.A, v.V().([]byte))
		v, err = doc.GetByField("b")
		require.NoError(t, err)
		require.Equal(t, u.B, v.V().(string))
		v, err = doc.GetByField("c")
		require.NoError(t, err)
		require.Equal(t, u.C, v.V().(bool))
		v, err = doc.GetByField("la-reponse-d")
		require.NoError(t, err)
		require.EqualValues(t, u.D, v.V().(int64))
		v, err = doc.GetByField("e")
		require.NoError(t, err)
		require.EqualValues(t, u.E, v.V().(int64))
		v, err = doc.GetByField("f")
		require.NoError(t, err)
		require.EqualValues(t, u.F, v.V().(int64))
		v, err = doc.GetByField("g")
		require.NoError(t, err)
		require.EqualValues(t, u.G, v.V().(int64))
		v, err = doc.GetByField("h")
		require.NoError(t, err)
		require.EqualValues(t, u.H, v.V().(int64))
		v, err = doc.GetByField("i")
		require.NoError(t, err)
		require.EqualValues(t, u.I, v.V().(int64))
		v, err = doc.GetByField("j")
		require.NoError(t, err)
		require.EqualValues(t, u.J, v.V().(int64))
		v, err = doc.GetByField("k")
		require.NoError(t, err)
		require.EqualValues(t, u.K, v.V().(int64))
		v, err = doc.GetByField("l")
		require.NoError(t, err)
		require.EqualValues(t, u.L, v.V().(int64))
		v, err = doc.GetByField("m")
		require.NoError(t, err)
		require.EqualValues(t, u.M, v.V().(int64))
		v, err = doc.GetByField("n")
		require.NoError(t, err)
		require.Equal(t, u.N, v.V().(float64))

		v, err = doc.GetByField("o")
		require.NoError(t, err)
		d, ok := v.V().(document.Document)
		require.True(t, ok)
		v, err = d.GetByField("ig")
		require.NoError(t, err)
		require.EqualValues(t, 0, v.V().(int64))

		v, err = doc.GetByField("ig")
		require.NoError(t, err)
		require.EqualValues(t, 100, v.V().(int64))

		v, err = doc.GetByField("t")
		require.NoError(t, err)
		a, ok := v.V().(document.Array)
		require.True(t, ok)
		var count int
		err = a.Iterate(func(i int, v document.Value) error {
			count++
			require.EqualValues(t, i+1, v.V().(int64))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, count)
		v, err = a.GetByIndex(10)
		require.Equal(t, err, document.ErrFieldNotFound)
		v, err = a.GetByIndex(1)
		require.NoError(t, err)
		require.EqualValues(t, 2, v.V().(int64))

		v, err = doc.GetByField("bb")
		require.NoError(t, err)
		var timeStr string
		require.NoError(t, v.Scan(&timeStr))
		parsedTime, err := time.Parse(time.RFC3339Nano, timeStr)
		require.NoError(t, err)
		require.Equal(t, u.BB, parsedTime)
	})

	t.Run("pointers", func(t *testing.T) {
		type s struct {
			A *int
		}

		d, err := document.NewFromStruct(new(s))
		require.NoError(t, err)
		_, err = d.GetByField("a")
		require.Equal(t, document.ErrFieldNotFound, err)

		a := 10
		ss := s{A: &a}
		d, err = document.NewFromStruct(&ss)
		require.NoError(t, err)
		v, err := d.GetByField("a")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(10), v)
	})
}

type foo struct {
	A string
	B int64
	C bool
	D float64
}

func (f *foo) Iterate(fn func(field string, value document.Value) error) error {
	var err error

	err = fn("a", document.NewTextValue(f.A))
	if err != nil {
		return err
	}

	err = fn("b", document.NewIntegerValue(f.B))
	if err != nil {
		return err
	}

	err = fn("c", document.NewBoolValue(f.C))
	if err != nil {
		return err
	}

	err = fn("d", document.NewDoubleValue(f.D))
	if err != nil {
		return err
	}

	return nil
}

func (f *foo) GetByField(field string) (document.Value, error) {
	switch field {
	case "a":
		return document.NewTextValue(f.A), nil
	case "b":
		return document.NewIntegerValue(f.B), nil
	case "c":
		return document.NewBoolValue(f.C), nil
	case "d":
		return document.NewDoubleValue(f.D), nil
	}

	return document.Value{}, errors.New("unknown field")
}

func TestPath(t *testing.T) {
	tests := []struct {
		name   string
		data   string
		path   string
		result string
		fails  bool
	}{
		{"root", `{"a": {"b": [1, 2, 3]}}`, `a`, `{"b": [1, 2, 3]}`, false},
		{"nested doc", `{"a": {"b": [1, 2, 3]}}`, `a.b`, `[1, 2, 3]`, false},
		{"nested array", `{"a": {"b": [1, 2, 3]}}`, `a.b[1]`, `2`, false},
		{"index out of range", `{"a": {"b": [1, 2, 3]}}`, `a.b[1000]`, ``, true},
		{"number field", `{"a": {"0": [1, 2, 3]}}`, "a.`0`", `[1, 2, 3]`, false},
		{"letter index", `{"a": {"b": [1, 2, 3]}}`, `a.b.c`, ``, true},
		{"unknown path", `{"a": {"b": [1, 2, 3]}}`, `a.e.f`, ``, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf document.FieldBuffer

			err := json.Unmarshal([]byte(test.data), &buf)
			require.NoError(t, err)
			p, err := parser.ParsePath(test.path)
			require.NoError(t, err)
			v, err := p.GetValueFromDocument(&buf)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				res, err := json.Marshal(v)
				require.NoError(t, err)
				require.JSONEq(t, test.result, string(res))
			}
		})
	}
}

func TestJSONDocument(t *testing.T) {
	tests := []struct {
		name     string
		d        document.Document
		expected string
	}{
		{
			"Flat",
			document.NewFieldBuffer().
				Add("name", document.NewTextValue("John")).
				Add("age", document.NewIntegerValue(10)).
				Add(`"something with" quotes`, document.NewIntegerValue(10)),
			`{"name":"John","age":10,"\"something with\" quotes":10}`,
		},
		{
			"Nested",
			document.NewFieldBuffer().
				Add("name", document.NewTextValue("John")).
				Add("age", document.NewIntegerValue(10)).
				Add("address", document.NewDocumentValue(document.NewFieldBuffer().
					Add("city", document.NewTextValue("Ajaccio")).
					Add("country", document.NewTextValue("France")),
				)).
				Add("friends", document.NewArrayValue(
					document.NewValueBuffer().
						Append(document.NewTextValue("fred")).
						Append(document.NewTextValue("jamie")),
				)),
			`{"name":"John","age":10,"address":{"city":"Ajaccio","country":"France"},"friends":["fred","jamie"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.d)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(data))
			require.NoError(t, err)
		})
	}
}

func BenchmarkDocumentIterate(b *testing.B) {
	f := foo{
		A: "a",
		B: 1000,
		C: true,
		D: 1e10,
	}

	b.Run("Implementation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.Iterate(func(string, document.Value) error {
				return nil
			})
		}
	})

}
