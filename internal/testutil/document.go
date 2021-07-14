package testutil

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// MakeValue turns v into a document.Value.
func MakeValue(t testing.TB, v interface{}) document.Value {
	t.Helper()

	vv, err := document.NewValue(v)
	require.NoError(t, err)
	return vv
}

func MakeArrayValue(t testing.TB, vs ...interface{}) document.Value {
	t.Helper()

	vvs := []document.Value{}
	for _, v := range vs {
		vvs = append(vvs, MakeValue(t, v))
	}

	vb := document.NewValueBuffer(vvs...)

	return document.NewArrayValue(vb)
}

// MakeDocument creates a document from a json string.
func MakeDocument(t testing.TB, jsonDoc string) document.Document {
	t.Helper()

	var fb document.FieldBuffer

	err := fb.UnmarshalJSON([]byte(jsonDoc))
	require.NoError(t, err)

	return &fb
}

// MakeDocuments creates a slice of document from json strings.
func MakeDocuments(t testing.TB, jsonDocs ...string) (docs Docs) {
	for _, jsonDoc := range jsonDocs {
		docs = append(docs, MakeDocument(t, jsonDoc))
	}
	return
}

// MakeArray creates an array from a json string.
func MakeArray(t testing.TB, jsonArray string) document.Array {
	t.Helper()

	var vb document.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	require.NoError(t, err)

	return &vb
}

func MakeValueBuffer(t testing.TB, jsonArray string) *document.ValueBuffer {
	t.Helper()

	var vb document.ValueBuffer

	err := vb.UnmarshalJSON([]byte(jsonArray))
	require.NoError(t, err)

	return &vb
}

type Docs []document.Document

func (docs Docs) RequireEqual(t testing.TB, others Docs) {
	t.Helper()

	require.Equal(t, len(docs), len(others), stringutil.Sprintf("expected len %d, got %d", len(docs), len(others)))

	for i, d := range docs {
		RequireDocEqual(t, d, others[i])
	}
}

// Dump a json representation of v to os.Stdout.
func Dump(t testing.TB, v interface{}) {
	t.Helper()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err := enc.Encode(v)
	require.NoError(t, err)
}

func RequireDocJSONEq(t testing.TB, d document.Document, expected string) {
	t.Helper()

	data, err := json.Marshal(d)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(data))
}

// IteratorToJSONArray encodes all the documents of an iterator to a JSON array.
func IteratorToJSONArray(w io.Writer, s document.Iterator) error {
	buf := bufio.NewWriter(w)

	buf.WriteByte('[')

	first := true
	err := s.Iterate(func(d document.Document) error {
		if !first {
			buf.WriteString(", ")
		} else {
			first = false
		}

		data, err := document.MarshalJSON(d)
		if err != nil {
			return err
		}

		_, err = buf.Write(data)
		return err
	})
	if err != nil {
		return err
	}

	buf.WriteByte(']')
	return buf.Flush()
}

func RequireDocEqual(t testing.TB, d1, d2 document.Document) {
	t.Helper()

	l := document.NewDocumentValue(d1)
	r := document.NewDocumentValue(d2)
	ok, err := document.IsEqual(l, r)
	require.NoError(t, err)
	if !ok {
		t.Fatal(cmp.Diff(transformDoc(d1), transformDoc(d2)))
	}
}
