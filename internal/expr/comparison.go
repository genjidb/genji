package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stringutil"
)

// A cmpOp is a comparison operator.
type cmpOp struct {
	*simpleOperator
}

// newCmpOp creates a comparison operator.
func newCmpOp(a, b Expr, t scanner.Token) *cmpOp {
	return &cmpOp{&simpleOperator{a, b, t}}
}

// Eval compares a and b together using the operator specified when constructing the CmpOp
// and returns the result of the comparison.
// Comparing with NULL always evaluates to NULL.
func (op *cmpOp) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		if a.Type() == document.NullValue || b.Type() == document.NullValue {
			return NullLiteral, nil
		}

		ok, err := op.compare(a, b)
		if ok {
			return TrueLiteral, err
		}

		return FalseLiteral, err
	})
}

func (op *cmpOp) compare(l, r document.Value) (bool, error) {
	switch op.Tok {
	case scanner.EQ:
		return document.IsEqual(l, r)
	case scanner.NEQ:
		return document.IsNotEqual(l, r)
	case scanner.GT:
		return document.IsGreaterThan(l, r)
	case scanner.GTE:
		return document.IsGreaterThanOrEqual(l, r)
	case scanner.LT:
		return document.IsLesserThan(l, r)
	case scanner.LTE:
		return document.IsLesserThanOrEqual(l, r)
	default:
		panic(stringutil.Sprintf("unknown token %v", op.Tok))
	}
}

// Eq creates an expression that returns true if a equals b.
func Eq(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.EQ)
}

// Neq creates an expression that returns true if a equals b.
func Neq(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.NEQ)
}

// Gt creates an expression that returns true if a is greater than b.
func Gt(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.GT)
}

// Gte creates an expression that returns true if a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.GTE)
}

// Lt creates an expression that returns true if a is lesser than b.
func Lt(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.LT)
}

// Lte creates an expression that returns true if a is lesser than or equal to b.
func Lte(a, b Expr) Expr {
	return newCmpOp(a, b, scanner.LTE)
}

type BetweenOperator struct {
	*simpleOperator
	X Expr
}

// Between returns a function that creates a BETWEEN operator that
// returns true if x is between a and b.
func Between(a Expr) func(x, b Expr) Expr {
	return func(x, b Expr) Expr {
		return &BetweenOperator{&simpleOperator{a, b, scanner.BETWEEN}, x}
	}
}

func (op *BetweenOperator) Eval(env *environment.Environment) (document.Value, error) {
	x, err := op.X.Eval(env)
	if err != nil {
		return FalseLiteral, err
	}

	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		if a.Type() == document.NullValue || b.Type() == document.NullValue {
			return NullLiteral, nil
		}

		ok, err := document.IsGreaterThanOrEqual(x, a)
		if !ok || err != nil {
			return FalseLiteral, err
		}

		ok, err = document.IsLesserThanOrEqual(x, b)
		if !ok || err != nil {
			return FalseLiteral, err
		}

		return TrueLiteral, nil
	})
}

func (op *BetweenOperator) String() string {
	return stringutil.Sprintf("%v BETWEEN %v AND %v", op.X, op.a, op.b)
}

// IsComparisonOperator returns true if e is one of
// =, !=, >, >=, <, <=, IS, IS NOT, IN, or NOT IN operators.
func IsComparisonOperator(op Operator) bool {
	switch op.(type) {
	case *cmpOp, *IsOperator, *IsNotOperator, *InOperator, *NotInOperator, *LikeOperator, *NotLikeOperator, *BetweenOperator:
		return true
	}

	return false
}

type InOperator struct {
	*simpleOperator
}

// In creates an expression that evaluates to the result of a IN b.
func In(a, b Expr) Expr {
	return &InOperator{&simpleOperator{a, b, scanner.IN}}
}

func (op *InOperator) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		if a.Type() == document.NullValue || b.Type() == document.NullValue {
			return NullLiteral, nil
		}

		if b.Type() != document.ArrayValue {
			return FalseLiteral, nil
		}

		ok, err := document.ArrayContains(b.V().(document.Array), a)
		if err != nil {
			return NullLiteral, err
		}

		if ok {
			return TrueLiteral, nil
		}
		return FalseLiteral, nil
	})
}

type NotInOperator struct {
	InOperator
}

// NotIn creates an expression that evaluates to the result of a NOT IN b.
func NotIn(a, b Expr) Expr {
	return &NotInOperator{InOperator{&simpleOperator{a, b, scanner.NIN}}}
}

func (op *NotInOperator) Eval(env *environment.Environment) (document.Value, error) {
	return invertBoolResult(op.InOperator.Eval)(env)
}

func (op *NotInOperator) String() string {
	return stringutil.Sprintf("%v NOT IN %v", op.a, op.b)
}

type IsOperator struct {
	*simpleOperator
}

// Is creates an expression that evaluates to the result of a IS b.
func Is(a, b Expr) Expr {
	return &IsOperator{&simpleOperator{a, b, scanner.IN}}
}

func (op *IsOperator) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		ok, err := document.IsEqual(a, b)
		if err != nil {
			return NullLiteral, err
		}
		if ok {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

type IsNotOperator struct {
	*simpleOperator
}

// IsNot creates an expression that evaluates to the result of a IS NOT b.
func IsNot(a, b Expr) Expr {
	return &IsNotOperator{&simpleOperator{a, b, scanner.ISN}}
}

func (op *IsNotOperator) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		ok, err := document.IsNotEqual(a, b)
		if err != nil {
			return NullLiteral, err
		}
		if ok {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

func (op *IsNotOperator) String() string {
	return stringutil.Sprintf("%v IS NOT %v", op.a, op.b)
}
