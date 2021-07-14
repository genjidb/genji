package statement

import (
	"errors"

	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
)

// SelectStmt holds SELECT configuration.
type SelectStmt struct {
	TableName        string
	Distinct         bool
	WhereExpr        expr.Expr
	GroupByExpr      expr.Expr
	OrderBy          expr.Path
	OrderByDirection scanner.Token
	OffsetExpr       expr.Expr
	LimitExpr        expr.Expr
	ProjectionExprs  []expr.Expr
	Union            struct {
		All        bool
		SelectStmt *StreamStmt
	}
}

func (stmt *SelectStmt) ToStream() (*StreamStmt, error) {
	isReadOnly := true

	var s *stream.Stream

	if stmt.TableName != "" {
		s = stream.New(stream.SeqScan(stmt.TableName))
	}

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if stmt.GroupByExpr != nil {
		// add Group node
		s = s.Pipe(stream.GroupBy(stmt.GroupByExpr))

		var invalidProjectedField expr.Expr
		var aggregators []expr.AggregatorBuilder

		for _, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				invalidProjectedField = pe
				break
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
				continue
			}

			// check if this is the same expression as the one used in the GROUP BY clause
			if expr.Equal(e, stmt.GroupByExpr) {
				continue
			}

			// otherwise it's an error
			invalidProjectedField = ne
			break
		}

		if invalidProjectedField != nil {
			return nil, stringutil.Errorf("field %q must appear in the GROUP BY clause or be used in an aggregate function", invalidProjectedField)
		}

		// add Aggregation node
		s = s.Pipe(stream.HashAggregate(aggregators...))
	} else {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []expr.AggregatorBuilder

		for _, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				continue
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
			}
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			s = s.Pipe(stream.HashAggregate(aggregators...))
		}
	}

	// If there is no FROM clause ensure there is no wildcard or path
	if stmt.TableName == "" {
		var err error

		for _, e := range stmt.ProjectionExprs {
			expr.Walk(e, func(e expr.Expr) bool {
				switch e.(type) {
				case expr.Path, expr.Wildcard:
					err = errors.New("no tables specified")
					return false
				default:
					return true
				}
			})
			if err != nil {
				return nil, err
			}
		}

		// build a document expression with the
		// projected exprs
		d := expr.KVPairs{
			Pairs: make([]expr.KVPair, len(stmt.ProjectionExprs)),
		}

		for i := range stmt.ProjectionExprs {
			d.Pairs[i].K = stmt.ProjectionExprs[i].String()
			d.Pairs[i].V = stmt.ProjectionExprs[i]
		}

		s = s.Pipe(stream.Expressions(&d))
	} else {
		s = s.Pipe(stream.Project(stmt.ProjectionExprs...))
	}

	if stmt.Distinct {
		s = s.Pipe(stream.Distinct())
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

	if stmt.Union.SelectStmt != nil {
		s = stream.New(stream.Concat(s, stmt.Union.SelectStmt.Stream))
	}

	// SELECT is read-only most of the time, unless it's using some expressions
	// that require write access and that are allowed to be run, such as NEXT VALUE FOR
	for _, e := range stmt.ProjectionExprs {
		expr.Walk(e, func(e expr.Expr) bool {
			switch e.(type) {
			case expr.NextValueFor:
				isReadOnly = false
				return false
			default:
				return true
			}
		})
	}

	return &StreamStmt{
		Stream:   s,
		ReadOnly: isReadOnly,
	}, nil
}
