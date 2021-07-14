package statement

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stream"
)

// ExplainStmt is a Statement that
// displays information about how a statement
// is going to be executed, without executing it.
type ExplainStmt struct {
	Statement Statement
}

// Run analyses the inner statement and displays its execution plan.
// If the statement is a stream, Optimize will be called prior to
// displaying all the operations.
// Explain currently only works on SELECT, UPDATE, INSERT and DELETE statements.
func (stmt *ExplainStmt) Run(ctx *Context) (Result, error) {
	st, ok := stmt.Statement.(*StreamStmt)
	if !ok {
		return Result{}, errors.New("EXPLAIN only works on INSERT, SELECT, UPDATE AND DELETE statements")
	}

	err := st.Prepare(ctx)
	if err != nil {
		return Result{}, err
	}

	var plan string
	if st.PreparedStream != nil {
		plan = st.PreparedStream.String()
	} else {
		plan = "<no exec>"
	}

	newStatement := StreamStmt{
		PreparedStream: &stream.Stream{
			Op: stream.Project(
				&expr.NamedExpr{
					ExprName: "plan",
					Expr:     expr.LiteralValue{Value: document.NewTextValue(plan)},
				}),
		},
		ReadOnly: true,
	}
	return newStatement.Run(ctx)
}

// IsReadOnly indicates that this statement doesn't write anything into
// the database.
func (s *ExplainStmt) IsReadOnly() bool {
	return true
}
