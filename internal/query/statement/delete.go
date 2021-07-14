package statement

import (
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
)

// DeleteConfig holds DELETE configuration.
type DeleteStmt struct {
	TableName        string
	WhereExpr        expr.Expr
	OffsetExpr       expr.Expr
	OrderBy          expr.Path
	LimitExpr        expr.Expr
	OrderByDirection scanner.Token
}

func (stmt *DeleteStmt) ToStream() (*StreamStmt, error) {
	s := stream.New(stream.SeqScan(stmt.TableName))

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	if stmt.OrderBy != nil {
		if stmt.OrderByDirection == scanner.DESC {
			s = s.Pipe(stream.SortReverse(stmt.OrderBy))
		} else {
			s = s.Pipe(stream.Sort(stmt.OrderBy))
		}
	}

	if stmt.OffsetExpr != nil {
		v, err := stmt.OffsetExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("offset expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Skip(v.V().(int64)))
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type.IsNumber() {
			return nil, stringutil.Errorf("limit expression must evaluate to a number, got %q", v.Type)
		}

		v, err = v.CastAsInteger()
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Take(v.V().(int64)))
	}

	s = s.Pipe(stream.TableDelete(stmt.TableName))

	return &StreamStmt{
		Stream:   s,
		ReadOnly: false,
	}, nil
}
