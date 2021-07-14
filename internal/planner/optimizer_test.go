package planner_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/planner"
	"github.com/genjidb/genji/internal/sql/parser"
	st "github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestSplitANDConditionRule(t *testing.T) {
	tests := []struct {
		name         string
		in, expected *st.Stream
	}{
		{
			"no and",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(testutil.BoolValue(true))),
			st.New(st.SeqScan("foo")).Pipe(st.Filter(testutil.BoolValue(true))),
		},
		{
			"and / top-level selection node",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(
				expr.And(
					testutil.BoolValue(true),
					testutil.BoolValue(false),
				),
			)),
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(testutil.BoolValue(true))).
				Pipe(st.Filter(testutil.BoolValue(false))),
		},
		{
			"and / middle-level selection node",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.And(
						testutil.BoolValue(true),
						testutil.BoolValue(false),
					),
				)).
				Pipe(st.Take(1)),
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(testutil.BoolValue(true))).
				Pipe(st.Filter(testutil.BoolValue(false))).
				Pipe(st.Take(1)),
		},
		{
			"multi and",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.And(
						expr.And(
							testutil.IntegerValue(1),
							testutil.IntegerValue(2),
						),
						expr.And(
							testutil.IntegerValue(3),
							testutil.IntegerValue(4),
						),
					),
				)).
				Pipe(st.Take(10)),
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(testutil.IntegerValue(1))).
				Pipe(st.Filter(testutil.IntegerValue(2))).
				Pipe(st.Filter(testutil.IntegerValue(3))).
				Pipe(st.Filter(testutil.IntegerValue(4))).
				Pipe(st.Take(10)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.SplitANDConditionRule(test.in, nil)
			require.NoError(t, err)
			require.Equal(t, res.String(), test.expected.String())
		})
	}
}

func TestPrecalculateExprRule(t *testing.T) {
	tests := []struct {
		name        string
		e, expected expr.Expr
	}{
		{
			"constant expr: 3 -> 3",
			testutil.IntegerValue(3),
			testutil.IntegerValue(3),
		},
		{
			"operator with two constant operands: 3 + 2.4 -> 5.4",
			expr.Add(testutil.IntegerValue(3), testutil.DoubleValue(2.4)),
			testutil.DoubleValue(5.4),
		},
		{
			"operator with constant nested operands: 3 > 1 - 40 -> true",
			expr.Gt(testutil.DoubleValue(3), expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))),
			testutil.BoolValue(true),
		},
		{
			"constant sub-expr: a > 1 - 40 -> a > -39",
			expr.Gt(expr.Path{document.PathFragment{FieldName: "a"}}, expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))),
			expr.Gt(expr.Path{document.PathFragment{FieldName: "a"}}, testutil.DoubleValue(-39)),
		},
		{
			"constant sub-expr: a IN [1, 2] -> a IN array([1, 2])",
			expr.In(expr.Path{document.PathFragment{FieldName: "a"}}, expr.LiteralExprList{testutil.IntegerValue(1), testutil.IntegerValue(2)}),
			expr.In(expr.Path{document.PathFragment{FieldName: "a"}}, expr.LiteralValue{Value: document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntegerValue(1)).
				Append(document.NewIntegerValue(2)))}),
		},
		{
			"non-constant expr list: [a, 1 - 40] -> [a, -39]",
			expr.LiteralExprList{
				expr.Path{document.PathFragment{FieldName: "a"}},
				expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40)),
			},
			expr.LiteralExprList{
				expr.Path{document.PathFragment{FieldName: "a"}},
				testutil.DoubleValue(-39),
			},
		},
		{
			"constant expr list: [3, 1 - 40] -> array([3, -39])",
			expr.LiteralExprList{
				testutil.IntegerValue(3),
				expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40)),
			},
			expr.LiteralValue{Value: document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntegerValue(3)).
				Append(document.NewDoubleValue(-39)))},
		},
		{
			`non-constant kvpair: {"a": d, "b": 1 - 40} -> {"a": 3, "b": -39}`,
			&expr.KVPairs{Pairs: []expr.KVPair{
				{K: "a", V: expr.Path{document.PathFragment{FieldName: "d"}}},
				{K: "b", V: expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))},
			}},
			&expr.KVPairs{Pairs: []expr.KVPair{
				{K: "a", V: expr.Path{document.PathFragment{FieldName: "d"}}},
				{K: "b", V: testutil.DoubleValue(-39)},
			}},
		},
		{
			`constant kvpair: {"a": 3, "b": 1 - 40} -> document({"a": 3, "b": -39})`,
			&expr.KVPairs{Pairs: []expr.KVPair{
				{K: "a", V: testutil.IntegerValue(3)},
				{K: "b", V: expr.Sub(testutil.IntegerValue(1), testutil.DoubleValue(40))},
			}},
			expr.LiteralValue{Value: document.NewDocumentValue(document.NewFieldBuffer().
				Add("a", document.NewIntegerValue(3)).
				Add("b", document.NewDoubleValue(-39)),
			)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := st.New(st.SeqScan("foo")).
				Pipe(st.Filter(test.e))
			res, err := planner.PrecalculateExprRule(s, nil)
			require.NoError(t, err)
			require.Equal(t, st.New(st.SeqScan("foo")).Pipe(st.Filter(test.expected)).String(), res.String())
		})
	}
}

func TestRemoveUnnecessarySelectionNodesRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *st.Stream
	}{
		{
			"non-constant expr",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a"))),
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a"))),
		},
		{
			"truthy constant expr",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("10"))),
			st.New(st.SeqScan("foo")),
		},
		{
			"truthy constant expr with IN",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(expr.In(
				expr.Path(document.NewPath("a")),
				testutil.ArrayValue(document.NewValueBuffer()),
			))),
			&st.Stream{},
		},
		{
			"falsy constant expr",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("0"))),
			&st.Stream{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := planner.RemoveUnnecessaryFilterNodesRule(test.root, nil)
			require.NoError(t, err)
			if test.expected != nil {
				require.Equal(t, test.expected.String(), res.String())
			} else {
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func TestRemoveUnnecessaryDedupNodeRule(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *st.Stream
	}{
		{
			"non-unique key",
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("b"))).
				Pipe(st.Distinct()),
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("b"))).
				Pipe(st.Distinct()),
		},
		{
			"primary key",
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("a"))).
				Pipe(st.Distinct()),
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("a"))),
		},
		{
			"primary key with alias",
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("a AS A"))).
				Pipe(st.Distinct()),
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("a AS A"))),
		},
		{
			"unique index",
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("c"))).
				Pipe(st.Distinct()),
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("c"))),
		},
		{
			"pk() function",
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("pk()"))).
				Pipe(st.Distinct()),
			st.New(st.SeqScan("foo")).
				Pipe(st.Project(parser.MustParseExpr("pk()"))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo(a integer PRIMARY KEY, b integer, c integer);
				CREATE UNIQUE INDEX idx_foo_idx ON foo(c);
				INSERT INTO foo (a, b, c) VALUES
					(1, 1, 1),
					(2, 2, 2),
					(3, 3, 3)
			`)

			res, err := planner.RemoveUnnecessaryDistinctNodeRule(test.root, db.Catalog)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), res.String())
		})
	}
}

func exprList(list ...expr.Expr) expr.LiteralExprList {
	return expr.LiteralExprList(list)
}

func TestUseIndexBasedOnSelectionNodeRule_Simple(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *st.Stream
	}{
		{
			"non-indexed path",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("d = 1"))),
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("d = 1"))),
		},
		{
			"FROM foo WHERE a = 1",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a = 1"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE c = 3 AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("c = 3"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.IntegerValue(3)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE c > 3 AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("c > 3"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
			st.New(st.IndexScan("idx_foo_b", st.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("c > 3"))),
		},
		{
			"SELECT a FROM foo WHERE c = 3 AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("c = 3"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.IntegerValue(3)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
		},
		{
			"SELECT a FROM foo WHERE c = 'hello' AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("c = 'hello'"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.TextValue("hello")), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
		},
		{
			"SELECT a FROM foo WHERE c = 'hello' AND d = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("c = 'hello'"))).
				Pipe(st.Filter(parser.MustParseExpr("d = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.TextValue("hello")), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("d = 2"))).
				Pipe(st.Project(parser.MustParseExpr("a"))),
		},
		{
			"FROM foo WHERE a IN [1, 2]",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(
				expr.In(
					parser.MustParseExpr("a"),
					testutil.ExprList(t, `[1, 2]`),
				),
			)),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true}, st.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exact: true})),
		},
		{
			"FROM foo WHERE 1 IN a",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("1 IN a"))),
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("1 IN a"))),
		},
		{
			"FROM foo WHERE a >= 10",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a >= 10"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: exprList(testutil.IntegerValue(10))})),
		},
		{
			"FROM foo WHERE k = 1",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("k = 1"))),
			st.New(st.PkScan("foo", st.ValueRange{Min: testutil.IntegerValue(1), Exact: true})),
		},
		{
			"FROM foo WHERE k = 1 AND b = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("k = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
			st.New(st.PkScan("foo", st.ValueRange{Min: testutil.IntegerValue(1), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND k = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("2 = k"))),
			st.New(st.PkScan("foo", st.ValueRange{Min: testutil.IntegerValue(2), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))),
		},
		{
			"FROM foo WHERE a = 1 AND k < 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("k < 2"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("k < 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND k = 'hello'",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("k = 'hello'"))),
			st.New(st.PkScan("foo", st.ValueRange{Min: testutil.TextValue("hello"), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))),
		},
		{ // c is an INT, 1.1 cannot be converted to int without precision loss, don't use the index
			"FROM foo WHERE c < 1.1",
			st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("c < 1.1"))),
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Max: exprList(testutil.DoubleValue(1.1)), Exclusive: true})),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo (k INT PRIMARY KEY, c INT);
				CREATE INDEX idx_foo_a ON foo(a);
				CREATE INDEX idx_foo_b ON foo(b);
				CREATE UNIQUE INDEX idx_foo_c ON foo(c);
				INSERT INTO foo (k, a, b, c, d) VALUES
					(1, 1, 1, 1, 1),
					(2, 2, 2, 2, 2),
					(3, 3, 3, 3, 3)
			`)

			res, err := planner.UseIndexBasedOnFilterNodeRule(test.root, db.Catalog)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), res.String())
		})
	}

	t.Run("array indexes", func(t *testing.T) {
		tests := []struct {
			name           string
			root, expected *st.Stream
		}{
			{
				"non-indexed path",
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("b = [1, 1]"))),
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("b = [1, 1]"))),
			},
			{
				"FROM foo WHERE k = [1, 1]",
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("k = [1, 1]"))),
				st.New(st.PkScan("foo", st.ValueRange{Min: testutil.ExprList(t, `[1, 1]`), Exact: true})),
			},
			{ // constraint on k[0] INT should not modify the operand
				"FROM foo WHERE k = [1.5, 1.5]",
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("k = [1.5, 1.5]"))),
				st.New(st.PkScan("foo", st.ValueRange{Min: testutil.ExprList(t, `[1.5, 1.5]`), Exact: true})),
			},
			{
				"FROM foo WHERE a = [1, 1]",
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a = [1, 1]"))),
				st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: testutil.ExprList(t, `[1, 1]`), Exact: true})),
			},
			{ // constraint on a[0] DOUBLE should modify the operand because it's lossless
				"FROM foo WHERE a = [1, 1.5]",
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a = [1, 1.5]"))),
				st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: testutil.ExprList(t, `[1.0, 1.5]`), Exact: true})),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				db, tx, cleanup := testutil.NewTestTx(t)
				defer cleanup()

				testutil.MustExec(t, db, tx, `
					CREATE TABLE foo (
						k ARRAY PRIMARY KEY,
						k[0] INT,
						a ARRAY,
						a[0] DOUBLE
					);
					CREATE INDEX idx_foo_a ON foo(a);
					CREATE INDEX idx_foo_a0 ON foo(a[0]);
					INSERT INTO foo (k, a, b) VALUES
						([1, 1], [1, 1], [1, 1]),
						([2, 2], [2, 2], [2, 2]),
						([3, 3], [3, 3], [3, 3])
				`)

				res, err := planner.PrecalculateExprRule(test.root, db.Catalog)
				require.NoError(t, err)

				res, err = planner.UseIndexBasedOnFilterNodeRule(res, db.Catalog)
				require.NoError(t, err)
				require.Equal(t, test.expected.String(), res.String())
			})
		}
	})
}

func TestUseIndexBasedOnSelectionNodeRule_Composite(t *testing.T) {
	tests := []struct {
		name           string
		root, expected *st.Stream
	}{
		{
			"FROM foo WHERE a = 1 AND d = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d = 2"))),
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d > 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d > 2"))),
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d < 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d < 2"))),
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Max: testutil.ExprList(t, `[1, 2]`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND d <= 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d <= 2"))),
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Max: testutil.ExprList(t, `[1, 2]`)})),
		},
		{
			"FROM foo WHERE a = 1 AND d >= 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d >= 2"))),
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`)})),
		},
		{
			"FROM foo WHERE a > 1 AND d > 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a > 1"))).
				Pipe(st.Filter(parser.MustParseExpr("d > 2"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: testutil.ExprList(t, `[1]`), Exclusive: true})).
				Pipe(st.Filter(parser.MustParseExpr("d > 2"))),
		},
		{
			"FROM foo WHERE a > ? AND d > ?",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a > ?"))).
				Pipe(st.Filter(parser.MustParseExpr("d > ?"))),
			st.New(st.IndexScan("idx_foo_a", st.IndexRange{Min: testutil.ExprList(t, `[?]`), Exclusive: true})).
				Pipe(st.Filter(parser.MustParseExpr("d > ?"))),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2 AND c = 3",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Filter(parser.MustParseExpr("c = 3"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Min: testutil.ExprList(t, `[1, 2, 3]`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2", // c is omitted, but it can still use idx_foo_a_b_c
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exact: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b > 2", // c is omitted, but it can still use idx_foo_a_b_c, with > b
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b > 2"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b < 2", // c is omitted, but it can still use idx_foo_a_b_c, with > b
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b < 2"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Max: testutil.ExprList(t, `[1, 2]`), Exclusive: true})),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2 and k = 3", // c is omitted, but it can still use idx_foo_a_b_c
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Filter(parser.MustParseExpr("k = 3"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("k = 3"))),
		},
		// If a path is missing from the query, we can still the index, with paths after the missing one are
		// using filter nodes rather than the index.
		{
			"FROM foo WHERE x = 1 AND z = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("x = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("z = 2"))),
			st.New(st.IndexScan("idx_foo_x_y_z", st.IndexRange{Min: exprList(testutil.IntegerValue(1)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("z = 2"))),
		},
		{
			"FROM foo WHERE a = 1 AND c = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("c = 2"))),
			// c will be picked because it's a unique index and thus has a lower cost
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))),
		},
		{
			"FROM foo WHERE b = 1 AND c = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("b = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("c = 2"))),
			// c will be picked because it's a unique index and thus has a lower cost
			st.New(st.IndexScan("idx_foo_c", st.IndexRange{Min: exprList(testutil.IntegerValue(2)), Exact: true})).
				Pipe(st.Filter(parser.MustParseExpr("b = 1"))),
		},
		{
			"FROM foo WHERE a = 1 AND b = 2 AND c = 'a'",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
				Pipe(st.Filter(parser.MustParseExpr("b = 2"))).
				Pipe(st.Filter(parser.MustParseExpr("c = 'a'"))),
			st.New(st.IndexScan("idx_foo_a_b_c", st.IndexRange{Min: exprList(testutil.IntegerValue(1), testutil.IntegerValue(2), testutil.TextValue("a")), Exact: true})),
		},

		{
			"FROM foo WHERE a IN [1, 2] AND d = 4",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `[1, 2]`),
					),
				)).
				Pipe(st.Filter(parser.MustParseExpr("d = 4"))),
			st.New(st.IndexScan("idx_foo_a_d",
				st.IndexRange{Min: testutil.ExprList(t, `[1, 4]`), Exact: true},
				st.IndexRange{Min: testutil.ExprList(t, `[2, 4]`), Exact: true},
			)),
		},
		{
			"FROM foo WHERE a IN [1, 2] AND b = 3 AND c = 4",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `[1, 2]`),
					),
				)).
				Pipe(st.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(st.Filter(parser.MustParseExpr("c = 4"))),
			st.New(st.IndexScan("idx_foo_a_b_c",
				st.IndexRange{Min: testutil.ExprList(t, `[1, 3, 4]`), Exact: true},
				st.IndexRange{Min: testutil.ExprList(t, `[2, 3, 4]`), Exact: true},
			)),
		},
		{
			"FROM foo WHERE a IN [1, 2] AND b = 3 AND c > 4",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `[1, 2]`),
					),
				)).
				Pipe(st.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(st.Filter(parser.MustParseExpr("c > 4"))),
			st.New(st.IndexScan("idx_foo_a_b_c",
				st.IndexRange{Min: testutil.ExprList(t, `[1, 3, 4]`), Exclusive: true},
				st.IndexRange{Min: testutil.ExprList(t, `[2, 3, 4]`), Exclusive: true},
			)),
		},
		{
			"FROM foo WHERE a IN [1, 2] AND b = 3 AND c < 4",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(
					expr.In(
						parser.MustParseExpr("a"),
						testutil.ExprList(t, `[1, 2]`),
					),
				)).
				Pipe(st.Filter(parser.MustParseExpr("b = 3"))).
				Pipe(st.Filter(parser.MustParseExpr("c < 4"))),
			st.New(st.IndexScan("idx_foo_a_b_c",
				st.IndexRange{Max: testutil.ExprList(t, `[1, 3, 4]`), Exclusive: true},
				st.IndexRange{Max: testutil.ExprList(t, `[2, 3, 4]`), Exclusive: true},
			)),
		},
		// {
		// 	"FROM foo WHERE a IN [1, 2] AND b IN [3, 4] AND c > 5",
		// 	st.New(st.SeqScan("foo")).
		// 		Pipe(st.Filter(
		// 			expr.In(
		// 				parser.MustParseExpr("a"),
		// 				testutil.ArrayValue(document.NewValueBuffer(document.NewIntegerValue(1), document.NewIntegerValue(2))),
		// 			),
		// 		)).
		// 		Pipe(st.Filter(
		// 			expr.In(
		// 				parser.MustParseExpr("b"),
		// 				testutil.ArrayValue(document.NewValueBuffer(document.NewIntegerValue(3), document.NewIntegerValue(4))),
		// 			),
		// 		)).
		// 		Pipe(st.Filter(parser.MustParseExpr("c < 5"))),
		// 	st.New(st.IndexScan("idx_foo_a_b_c",
		// 		st.IndexRange{Max: testutil.ExprList(t, `[1, 3, 5]`), Exclusive: true},
		// 		st.IndexRange{Max: testutil.ExprList(t, `[2, 3, 5]`), Exclusive: true},
		// 		st.IndexRange{Max: testutil.ExprList(t, `[1, 4, 5]`), Exclusive: true},
		// 		st.IndexRange{Max: testutil.ExprList(t, `[2, 4, 5]`), Exclusive: true},
		// 	)),
		// },
		{
			"FROM foo WHERE 1 IN a AND d = 2",
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("1 IN a"))).
				Pipe(st.Filter(parser.MustParseExpr("d = 4"))),
			st.New(st.SeqScan("foo")).
				Pipe(st.Filter(parser.MustParseExpr("1 IN a"))).
				Pipe(st.Filter(parser.MustParseExpr("d = 4"))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()

			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo (k INT PRIMARY KEY, c INT);
				CREATE INDEX idx_foo_a ON foo(a);
				CREATE INDEX idx_foo_b ON foo(b);
				CREATE UNIQUE INDEX idx_foo_c ON foo(c);
				CREATE INDEX idx_foo_a_d ON foo(a, d);
				CREATE INDEX idx_foo_a_b_c ON foo(a, b, c);
				CREATE INDEX idx_foo_x_y_z ON foo(x, y, z);
				INSERT INTO foo (k, a, b, c, d) VALUES
					(1, 1, 1, 1, 1),
					(2, 2, 2, 2, 2),
					(3, 3, 3, 3, 3)
			`)

			res, err := planner.UseIndexBasedOnFilterNodeRule(test.root, db.Catalog)
			require.NoError(t, err)
			require.Equal(t, test.expected.String(), res.String())
		})
	}

	t.Run("array indexes", func(t *testing.T) {
		tests := []struct {
			name           string
			root, expected *st.Stream
		}{
			{
				"FROM foo WHERE a = [1, 1] AND b = [2, 2]",
				st.New(st.SeqScan("foo")).
					Pipe(st.Filter(parser.MustParseExpr("a = [1, 1]"))).
					Pipe(st.Filter(parser.MustParseExpr("b = [2, 2]"))),
				st.New(st.IndexScan("idx_foo_a_b", st.IndexRange{
					Min:   testutil.ExprList(t, `[[1, 1], [2, 2]]`),
					Exact: true})),
			},
			{
				"FROM foo WHERE a = [1, 1] AND b > [2, 2]",
				st.New(st.SeqScan("foo")).
					Pipe(st.Filter(parser.MustParseExpr("a = [1, 1]"))).
					Pipe(st.Filter(parser.MustParseExpr("b > [2, 2]"))),
				st.New(st.IndexScan("idx_foo_a_b", st.IndexRange{
					Min:       testutil.ExprList(t, `[[1, 1], [2, 2]]`),
					Exclusive: true})),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				db, tx, cleanup := testutil.NewTestTx(t)
				defer cleanup()

				testutil.MustExec(t, db, tx, `
						CREATE TABLE foo (
							k ARRAY PRIMARY KEY,
							a ARRAY
						);
						CREATE INDEX idx_foo_a_b ON foo(a, b);
						CREATE INDEX idx_foo_a0 ON foo(a[0]);
						INSERT INTO foo (k, a, b) VALUES
							([1, 1], [1, 1], [1, 1]),
							([2, 2], [2, 2], [2, 2]),
							([3, 3], [3, 3], [3, 3])
	`)

				res, err := planner.PrecalculateExprRule(test.root, db.Catalog)
				require.NoError(t, err)

				res, err = planner.UseIndexBasedOnFilterNodeRule(res, db.Catalog)
				require.NoError(t, err)
				require.Equal(t, test.expected.String(), res.String())
			})
		}
	})
}

func TestOptimize(t *testing.T) {
	t.Run("concat operator operands are optimized", func(t *testing.T) {
		t.Run("PrecalculateExprRule", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()
			testutil.MustExec(t, db, tx, `
						CREATE TABLE foo;
						CREATE TABLE bar;
			`)

			got, err := planner.Optimize(
				st.New(st.Concat(
					st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a = 1 + 2"))),
					st.New(st.SeqScan("bar")).Pipe(st.Filter(parser.MustParseExpr("b = 1 + 2"))),
				)),
				db.Catalog)

			want := st.New(st.Concat(
				st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("a = 3"))),
				st.New(st.SeqScan("bar")).Pipe(st.Filter(parser.MustParseExpr("b = 3"))),
			))

			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
		})

		t.Run("RemoveUnnecessarySelectionNodesRule", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()
			testutil.MustExec(t, db, tx, `
						CREATE TABLE foo;
						CREATE TABLE bar;
			`)

			got, err := planner.Optimize(
				st.New(st.Concat(
					st.New(st.SeqScan("foo")).Pipe(st.Filter(parser.MustParseExpr("10"))),
					st.New(st.Concat(
						st.New(st.SeqScan("bar")).Pipe(st.Filter(parser.MustParseExpr("11"))),
						st.New(st.SeqScan("bar")).Pipe(st.Filter(parser.MustParseExpr("12"))),
					)),
				)),
				db.Catalog)

			want := st.New(st.Concat(
				st.New(st.SeqScan("foo")),
				st.New(st.Concat(
					st.New(st.SeqScan("bar")),
					st.New(st.SeqScan("bar")),
				)),
			))

			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
		})

		t.Run("RemoveUnnecessaryDedupNodeRule", func(t *testing.T) {
			db, tx, cleanup := testutil.NewTestTx(t)
			defer cleanup()
			testutil.MustExec(t, db, tx, `
				CREATE TABLE foo(a integer PRIMARY KEY);
				CREATE TABLE bar(a integer PRIMARY KEY);
			`)

			got, err := planner.Optimize(
				st.New(st.Concat(
					st.New(st.SeqScan("foo")).
						Pipe(st.Project(parser.MustParseExpr("a"))).
						Pipe(st.Distinct()),
					st.New(st.SeqScan("bar")).
						Pipe(st.Project(parser.MustParseExpr("a"))).
						Pipe(st.Distinct()),
				)),
				db.Catalog)

			want := st.New(st.Concat(
				st.New(st.SeqScan("foo")).
					Pipe(st.Project(parser.MustParseExpr("a"))),
				st.New(st.SeqScan("bar")).
					Pipe(st.Project(parser.MustParseExpr("a"))),
			))

			require.NoError(t, err)
			require.Equal(t, want.String(), got.String())
		})
	})

	t.Run("UseIndexBasedOnSelectionNodeRule", func(t *testing.T) {
		db, tx, cleanup := testutil.NewTestTx(t)
		defer cleanup()
		testutil.MustExec(t, db, tx, `
				CREATE TABLE foo;
				CREATE TABLE bar;
				CREATE INDEX idx_foo_a_d ON foo(a, d);
				CREATE INDEX idx_bar_a_d ON bar(a, d);
			`)

		got, err := planner.Optimize(
			st.New(st.Concat(
				st.New(st.SeqScan("foo")).
					Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
					Pipe(st.Filter(parser.MustParseExpr("d = 2"))),
				st.New(st.SeqScan("bar")).
					Pipe(st.Filter(parser.MustParseExpr("a = 1"))).
					Pipe(st.Filter(parser.MustParseExpr("d = 2"))),
			)),
			db.Catalog)

		want := st.New(st.Concat(
			st.New(st.IndexScan("idx_foo_a_d", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exact: true})),
			st.New(st.IndexScan("idx_bar_a_d", st.IndexRange{Min: testutil.ExprList(t, `[1, 2]`), Exact: true})),
		))

		require.NoError(t, err)
		require.Equal(t, want.String(), got.String())
	})
}
