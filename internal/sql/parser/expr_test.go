package parser_test

import (
	"strings"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestParserExpr(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		fails    bool
	}{
		// integers
		{"+int8", "10", testutil.IntegerValue(10), false},
		{"-int8", "-10", testutil.IntegerValue(-10), false},
		{"+int16", "1000", testutil.IntegerValue(1000), false},
		{"-int16", "-1000", testutil.IntegerValue(-1000), false},
		{"+int32", "10000000", testutil.IntegerValue(10000000), false},
		{"-int32", "-10000000", testutil.IntegerValue(-10000000), false},
		{"+int64", "10000000000", testutil.IntegerValue(10000000000), false},
		{"-int64", "-10000000000", testutil.IntegerValue(-10000000000), false},
		{"> max int64 -> float64", "10000000000000000000", testutil.DoubleValue(10000000000000000000), false},
		{"< min int64 -> float64", "-10000000000000000000", testutil.DoubleValue(-10000000000000000000), false},
		{"very large int", "100000000000000000000000000000000000000000000000", testutil.DoubleValue(100000000000000000000000000000000000000000000000), false},

		// floats
		{"+float64", "10.0", testutil.DoubleValue(10), false},
		{"-float64", "-10.0", testutil.DoubleValue(-10), false},

		// strings
		{"double quoted string", `"10.0"`, testutil.TextValue("10.0"), false},
		{"single quoted string", "'-10.0'", testutil.TextValue("-10.0"), false},

		// documents
		{"empty document", `{}`, &expr.KVPairs{SelfReferenced: true}, false},
		{"document values", `{a: 1, b: 1.0, c: true, d: 'string', e: "string", f: {foo: 'bar'}, g: h.i.j, k: [1, 2, 3]}`,
			&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
				{K: "a", V: testutil.IntegerValue(1)},
				{K: "b", V: testutil.DoubleValue(1)},
				{K: "c", V: testutil.BoolValue(true)},
				{K: "d", V: testutil.TextValue("string")},
				{K: "e", V: testutil.TextValue("string")},
				{K: "f", V: &expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
					{K: "foo", V: testutil.TextValue("bar")},
				}}},
				{K: "g", V: testutil.ParsePath(t, "h.i.j")},
				{K: "k", V: expr.LiteralExprList{testutil.IntegerValue(1), testutil.IntegerValue(2), testutil.IntegerValue(3)}},
			}},
			false},
		{"document keys", `{a: 1, "foo bar __&&))": 1, 'ola ': 1}`,
			&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{
				{K: "a", V: testutil.IntegerValue(1)},
				{K: "foo bar __&&))", V: testutil.IntegerValue(1)},
				{K: "ola ", V: testutil.IntegerValue(1)},
			}},
			false},
		{"bad document keys: same key", `{a: 1, a: 2, "a": 3}`, nil, true},
		{"bad document keys: param", `{?: 1}`, nil, true},
		{"bad document keys: dot", `{a.b: 1}`, nil, true},
		{"bad document keys: space", `{a b: 1}`, nil, true},
		{"bad document: missing right bracket", `{a: 1`, nil, true},
		{"bad document: missing colon", `{a: 1, 'b'}`, nil, true},

		// parentheses
		{"parentheses: empty", "()", nil, true},
		{"parentheses: values", `(1)`,
			expr.Parentheses{
				E: testutil.IntegerValue(1),
			}, false},
		{"parentheses: expr", `(1 + true * (4 + 3))`,
			expr.Parentheses{
				E: expr.Add(
					testutil.IntegerValue(1),
					expr.Mul(
						testutil.BoolValue(true),
						expr.Parentheses{
							E: expr.Add(
								testutil.IntegerValue(4),
								testutil.IntegerValue(3),
							),
						},
					),
				),
			}, false},
		{"list with brackets: empty", "[]", expr.LiteralExprList(nil), false},
		{"list with brackets: values", `[1, true, {a: 1}, a.b.c, (-1), [-1]]`,
			expr.LiteralExprList{
				testutil.IntegerValue(1),
				testutil.BoolValue(true),
				&expr.KVPairs{SelfReferenced: true, Pairs: []expr.KVPair{{K: "a", V: testutil.IntegerValue(1)}}},
				testutil.ParsePath(t, "a.b.c"),
				expr.Parentheses{E: testutil.IntegerValue(-1)},
				expr.LiteralExprList{testutil.IntegerValue(-1)},
			}, false},
		{"list with brackets: missing bracket", `[1, true, {a: 1}, a.b.c, (-1), [-1]`, nil, true},

		// operators
		{"=", "age = 10", expr.Eq(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"!=", "age != 10", expr.Neq(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{">", "age > 10", expr.Gt(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{">=", "age >= 10", expr.Gte(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"<", "age < 10", expr.Lt(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"<=", "age <= 10", expr.Lte(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"BETWEEN", "1 BETWEEN 10 AND 11", expr.Between(testutil.IntegerValue(10))(testutil.IntegerValue(1), testutil.IntegerValue(11)), false},
		{"+", "age + 10", expr.Add(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"-", "age - 10", expr.Sub(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"*", "age * 10", expr.Mul(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"/", "age / 10", expr.Div(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"%", "age % 10", expr.Mod(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"&", "age & 10", expr.BitwiseAnd(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)), false},
		{"||", "name || 'foo'", expr.Concat(testutil.ParsePath(t, "name"), testutil.TextValue("foo")), false},
		{"IN", "age IN ages", expr.In(testutil.ParsePath(t, "age"), testutil.ParsePath(t, "ages")), false},
		{"NOT IN", "age NOT IN ages", expr.NotIn(testutil.ParsePath(t, "age"), testutil.ParsePath(t, "ages")), false},
		{"IS", "age IS NULL", expr.Is(testutil.ParsePath(t, "age"), testutil.NullValue()), false},
		{"IS NOT", "age IS NOT NULL", expr.IsNot(testutil.ParsePath(t, "age"), testutil.NullValue()), false},
		{"LIKE", "name LIKE 'foo'", expr.Like(testutil.ParsePath(t, "name"), testutil.TextValue("foo")), false},
		{"NOT LIKE", "name NOT LIKE 'foo'", expr.NotLike(testutil.ParsePath(t, "name"), testutil.TextValue("foo")), false},
		{"NOT =", "name NOT = 'foo'", nil, true},
		{"precedence", "4 > 1 + 2", expr.Gt(
			testutil.IntegerValue(4),
			expr.Add(
				testutil.IntegerValue(1),
				testutil.IntegerValue(2),
			),
		), false},
		{"AND", "age = 10 AND age <= 11",
			expr.And(
				expr.Eq(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)),
				expr.Lte(testutil.ParsePath(t, "age"), testutil.IntegerValue(11)),
			), false},
		{"OR", "age = 10 OR age = 11",
			expr.Or(
				expr.Eq(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)),
				expr.Eq(testutil.ParsePath(t, "age"), testutil.IntegerValue(11)),
			), false},
		{"AND then OR", "age >= 10 AND age > $age OR age < 10.4",
			expr.Or(
				expr.And(
					expr.Gte(testutil.ParsePath(t, "age"), testutil.IntegerValue(10)),
					expr.Gt(testutil.ParsePath(t, "age"), expr.NamedParam("age")),
				),
				expr.Lt(testutil.ParsePath(t, "age"), testutil.DoubleValue(10.4)),
			), false},
		{"with NULL", "age > NULL", expr.Gt(testutil.ParsePath(t, "age"), testutil.NullValue()), false},

		// unary operators
		{"CAST", "CAST(a.b[1][0] AS TEXT)", expr.CastFunc{Expr: testutil.ParsePath(t, "a.b[1][0]"), CastAs: types.TextValue}, false},
		{"NOT", "NOT 10", expr.Not(testutil.IntegerValue(10)), false},
		{"NOT", "NOT NOT", nil, true},
		{"NOT", "NOT NOT 10", expr.Not(expr.Not(testutil.IntegerValue(10))), false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR hello", expr.NextValueFor{SeqName: "hello"}, false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR `good morning`", expr.NextValueFor{SeqName: "good morning"}, false},
		{"NEXT VALUE FOR", "NEXT VALUE FOR 10", nil, true},

		// functions
		{"pk() function", "pk()", &expr.PKFunc{}, false},
		{"count(expr) function", "count(a)", &expr.CountFunc{Expr: testutil.ParsePath(t, "a")}, false},
		{"count(*) function", "count(*)", &expr.CountFunc{Wildcard: true}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := parser.NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if !expr.Equal(test.expected, ex) {
					require.EqualValues(t, test.expected, ex)
				}
			}
		})
	}
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected document.Path
		fails    bool
	}{
		{"one fragment", `a`, document.Path{
			document.PathFragment{FieldName: "a"},
		}, false},
		{"one fragment with quotes", "`    \"a\"`", document.Path{
			document.PathFragment{FieldName: "    \"a\""},
		}, false},
		{"multiple fragments", `a.b[100].c[1][2]`, document.Path{
			document.PathFragment{FieldName: "a"},
			document.PathFragment{FieldName: "b"},
			document.PathFragment{ArrayIndex: 100},
			document.PathFragment{FieldName: "c"},
			document.PathFragment{ArrayIndex: 1},
			document.PathFragment{ArrayIndex: 2},
		}, false},
		{"with quotes", "`some ident`.` with`[5].`  \"quotes`", document.Path{
			document.PathFragment{FieldName: "some ident"},
			document.PathFragment{FieldName: " with"},
			document.PathFragment{ArrayIndex: 5},
			document.PathFragment{FieldName: "  \"quotes"},
		}, false},
		{"negative index", `a.b[-100].c`, nil, true},
		{"with spaces", `a.  b[100].  c`, nil, true},
		{"starting with array", `[10].a`, nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vp, err := parser.ParsePath(test.s)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, vp)
			}
		})
	}
}

func TestParserParams(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected expr.Expr
		errored  bool
	}{
		{"one positional", "age = ?", expr.Eq(testutil.ParsePath(t, "age"), expr.PositionalParam(1)), false},
		{"multiple positional", "age = ? AND age <= ?",
			expr.And(
				expr.Eq(testutil.ParsePath(t, "age"), expr.PositionalParam(1)),
				expr.Lte(testutil.ParsePath(t, "age"), expr.PositionalParam(2)),
			), false},
		{"one named", "age = $age", expr.Eq(testutil.ParsePath(t, "age"), expr.NamedParam("age")), false},
		{"multiple named", "age = $foo OR age = $bar",
			expr.Or(
				expr.Eq(testutil.ParsePath(t, "age"), expr.NamedParam("foo")),
				expr.Eq(testutil.ParsePath(t, "age"), expr.NamedParam("bar")),
			), false},
		{"mixed", "age >= ? AND age > $foo OR age < ?", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ex, err := parser.NewParser(strings.NewReader(test.s)).ParseExpr()
			if test.errored {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, test.expected, ex)
			}
		})
	}
}
