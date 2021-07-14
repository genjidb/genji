package testutil

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil/genexprtests"
	"github.com/stretchr/testify/require"
)

// BlobValue creates a literal value of type Blob.
func BlobValue(v []byte) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewBlobValue(v)}
}

// TextValue creates a literal value of type Text.
func TextValue(v string) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewTextValue(v)}
}

// BoolValue creates a literal value of type Bool.
func BoolValue(v bool) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewBoolValue(v)}
}

// IntegerValue creates a literal value of type Integer.
func IntegerValue(v int64) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewIntegerValue(v)}
}

// DoubleValue creates a literal value of type Double.
func DoubleValue(v float64) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewDoubleValue(v)}
}

// NullValue creates a literal value of type Null.
func NullValue() expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewNullValue()}
}

// DocumentValue creates a literal value of type Document.
func DocumentValue(d document.Document) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewDocumentValue(d)}
}

// ArrayValue creates a literal value of type Array.
func ArrayValue(a document.Array) expr.LiteralValue {
	return expr.LiteralValue{Value: document.NewArrayValue(a)}
}

func ExprList(t testing.TB, s string) expr.LiteralExprList {
	t.Helper()

	e, err := parser.ParseExpr(s)
	require.NoError(t, err)
	require.IsType(t, e, expr.LiteralExprList{})

	return e.(expr.LiteralExprList)
}

func ParsePath(t testing.TB, p string) expr.Path {
	t.Helper()

	return expr.Path(ParseDocumentPath(t, p))
}

func ParseDocumentPath(t testing.TB, p string) document.Path {
	t.Helper()

	vp, err := parser.ParsePath(p)
	require.NoError(t, err)
	return vp
}

func ParseNamedExpr(t testing.TB, s string, name ...string) expr.Expr {
	t.Helper()

	e, err := parser.ParseExpr(s)
	require.NoError(t, err)

	ne := expr.NamedExpr{
		Expr:     e,
		ExprName: s,
	}

	if len(name) > 0 {
		ne.ExprName = name[0]
	}

	return &ne
}

var emptyEnv = environment.New(nil)

func ExprRunner(t *testing.T, testfile string) {
	t.Helper()

	f, err := os.Open(testfile)
	if err != nil {
		t.Errorf("Failed to open test data, got %v (%s)", err, testfile)
	}

	ts, err := genexprtests.Parse(f)
	require.NoError(t, err)

	for _, test := range ts.Tests {
		t.Run(test.Name, func(t *testing.T) {
			for _, stmt := range test.Statements {
				if !stmt.Fail {
					t.Run("OK "+stmt.Expr, func(t *testing.T) {
						// parse the expected result
						e, err := parser.NewParser(strings.NewReader(stmt.Res)).ParseExpr()
						require.NoError(t, err)

						// eval it to get a proper Value
						want, err := e.Eval(emptyEnv)
						require.NoError(t, err)

						// parse the given epxr
						e, err = parser.NewParser(strings.NewReader(stmt.Expr)).ParseExpr()
						require.NoError(t, err)

						// eval it to get a proper Value
						got, err := e.Eval(emptyEnv)
						require.NoError(t, err)

						// finally, compare those two
						require.Equal(t, want, got)
					})
				} else {
					t.Run("NOK "+stmt.Expr, func(t *testing.T) {
						// parse the given epxr
						e, err := parser.NewParser(strings.NewReader(stmt.Expr)).ParseExpr()
						require.NoError(t, err)

						// eval it, it should return an error
						_, err = e.Eval(emptyEnv)
						require.NotNilf(t, err, "expected expr `%s` to return an error, got nil", stmt.Expr)
						require.Regexp(t, regexp.MustCompile(stmt.Res), err.Error())
					})
				}
			}
		})
	}
}
