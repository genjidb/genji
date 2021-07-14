package parser_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParserAlterTable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo RENAME TO bar", statement.AlterStmt{TableName: "foo", NewTableName: "bar"}, false},
		{"With error / missing TABLE keyword", "ALTER foo RENAME TO bar", statement.AlterStmt{}, true},
		{"With error / two identifiers for table name", "ALTER TABLE foo baz RENAME TO bar", statement.AlterStmt{}, true},
		{"With error / two identifiers for new table name", "ALTER TABLE foo RENAME TO bar baz", statement.AlterStmt{}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}

func TestParserAlterTableAddField(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected statement.Statement
		errored  bool
	}{
		{"Basic", "ALTER TABLE foo ADD FIELD bar", statement.AlterTableAddField{TableName: "foo",
			Constraint: database.FieldConstraint{},
		}, true},
		{"With type", "ALTER TABLE foo ADD FIELD bar integer", statement.AlterTableAddField{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path: document.Path(testutil.ParsePath(t, "bar")),
				Type: document.IntegerValue,
			},
		}, false},
		{"With not null", "ALTER TABLE foo ADD FIELD bar NOT NULL", statement.AlterTableAddField{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path:      document.Path(testutil.ParsePath(t, "bar")),
				IsNotNull: true,
			},
		}, false},
		{"With primary key", "ALTER TABLE foo ADD FIELD bar PRIMARY KEY", statement.AlterTableAddField{}, true},
		{"With multiple constraints", "ALTER TABLE foo ADD FIELD bar integer NOT NULL DEFAULT 0", statement.AlterTableAddField{TableName: "foo",
			Constraint: database.FieldConstraint{
				Path:         document.Path(testutil.ParsePath(t, "bar")),
				Type:         document.IntegerValue,
				IsNotNull:    true,
				DefaultValue: expr.Constraint(expr.LiteralValue{Value: document.NewIntegerValue(0)}),
			},
		}, false},
		{"With error / missing FIELD keyword", "ALTER TABLE foo ADD bar", nil, true},
		{"With error / missing field name", "ALTER TABLE foo ADD FIELD", nil, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if test.errored {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, q.Statements, 1)
			require.EqualValues(t, test.expected, q.Statements[0])
		})
	}
}
