package database_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestFieldConstraintsInfer(t *testing.T) {
	tests := []struct {
		name      string
		got, want database.FieldConstraints
		fails     bool
	}{
		{
			"No change",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			false,
		},
		{
			"Array",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "0"), Type: types.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "0")}},
				{Path: document.NewPath("a", "0"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Document",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "b"), Type: types.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b")}},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Primary key",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.ArrayValue, IsPrimaryKey: true},
				{Path: document.NewPath("a", "0"), Type: types.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.ArrayValue, IsPrimaryKey: true},
				{Path: document.NewPath("a", "0"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Complex path",
			[]*database.FieldConstraint{{Path: document.NewPath("a", "b", "3", "1", "c"), Type: types.IntegerValue}},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b"), Type: types.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3"), Type: types.ArrayValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Overlaping constraints",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: types.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{document.NewPath("a", "b"), document.NewPath("a", "c")}},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "c"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred first",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
				{Path: document.NewPath("a"), Type: types.DocumentValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Same path inferred and non inferred: inferred last",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Complex case",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: types.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: types.IntegerValue},
			},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d"), document.NewPath("a", "b", "2")}},
				{Path: document.NewPath("a", "b", "3"), Type: types.ArrayValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1"), Type: types.DocumentValue, IsInferred: true,
					InferredBy: []document.Path{document.NewPath("a", "b", "3", "1", "c"), document.NewPath("a", "b", "3", "1", "c", "d")}},
				{Path: document.NewPath("a", "b", "3", "1", "c"), Type: types.DocumentValue},
				{Path: document.NewPath("a", "b", "3", "1", "c", "d"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "b", "2"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Same path, different constraint",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: types.DoubleValue},
			},
			nil,
			true,
		},
		{
			"Inferred constraint first, conflict with non inferred",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
				{Path: document.NewPath("a"), Type: types.IntegerValue},
			},
			nil,
			true,
		},
		{
			"Non inferred constraint first, conflict with inferred",
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.IntegerValue},
				{Path: document.NewPath("a", "b"), Type: types.IntegerValue},
			},
			nil,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.got.Infer()
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestFieldConstraintsAdd(t *testing.T) {
	tests := []struct {
		name  string
		got   database.FieldConstraints
		add   database.FieldConstraint
		want  database.FieldConstraints
		fails bool
	}{
		{
			"Same path",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("a"), Type: types.IntegerValue},
			nil,
			true,
		},
		{
			"Duplicate primary key",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), IsPrimaryKey: true, Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), IsPrimaryKey: true, Type: types.IntegerValue},
			nil,
			true,
		},
		{
			"Different path",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: types.IntegerValue},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.IntegerValue},
				{Path: document.NewPath("b"), Type: types.IntegerValue},
			},
			false,
		},
		{
			"Default value conversion, typed constraint",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: types.IntegerValue, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.IntegerValue},
				{Path: document.NewPath("b"), Type: types.IntegerValue, DefaultValue: expr.Constraint(testutil.DoubleValue(5))},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: types.IntegerValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.IntegerValue},
				{Path: document.NewPath("b"), Type: types.IntegerValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			},
			false,
		},
		{
			"Default value conversion, typed constraint, NEXT VALUE FOR with blob",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: types.BlobValue, DefaultValue: expr.Constraint(expr.NextValueFor{SeqName: "seq"})},
			nil,
			true,
		},
		{
			"Default value conversion, typed constraint, incompatible value",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), Type: types.DoubleValue, DefaultValue: expr.Constraint(testutil.BoolValue(true))},
			nil,
			true,
		},
		{
			"Default value conversion, untyped constraint",
			[]*database.FieldConstraint{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			database.FieldConstraint{Path: document.NewPath("b"), DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			[]*database.FieldConstraint{
				{Path: document.NewPath("a"), Type: types.IntegerValue},
				{Path: document.NewPath("b"), DefaultValue: expr.Constraint(testutil.IntegerValue(5))},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.got.Add(&test.add)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, test.got)
			}
		})
	}
}

func TestFieldConstraintsConvert(t *testing.T) {
	tests := []struct {
		constraints database.FieldConstraints
		path        document.Path
		in, want    document.Value
		fails       bool
	}{
		{
			nil,
			document.NewPath("a"),
			document.NewIntegerValue(10),
			document.NewDoubleValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			document.NewPath("a"),
			document.NewIntegerValue(10),
			document.NewIntegerValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			document.NewPath("a"),
			document.NewDoubleValue(10.5),
			document.NewIntegerValue(10),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a", "0"), Type: types.IntegerValue}},
			document.NewPath("a"),
			document.NewArrayValue(testutil.MakeArray(t, `[10.5, 10.5]`)),
			document.NewArrayValue(testutil.MakeArray(t, `[10, 10.5]`)),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a", "b"), Type: types.IntegerValue}},
			document.NewPath("a"),
			document.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10.5, "c": 10.5}`)),
			document.NewDocumentValue(testutil.MakeDocument(t, `{"b": 10, "c": 10.5}`)),
			false,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), Type: types.IntegerValue}},
			document.NewPath("a"),
			document.NewTextValue("foo"),
			document.NewTextValue("foo"),
			true,
		},
		{
			database.FieldConstraints{{Path: document.NewPath("a"), DefaultValue: expr.Constraint(testutil.IntegerValue(10))}},
			document.NewPath("a"),
			document.NewTextValue("foo"),
			document.NewTextValue("foo"),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s / %v to %v", test.path, test.in, test.want), func(t *testing.T) {
			got, err := test.constraints.ConvertValueAtPath(test.path, test.in, database.CastConversion)
			if test.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			}
		})
	}
}
