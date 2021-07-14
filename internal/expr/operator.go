package expr

import (
	"errors"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stringutil"
)

type simpleOperator struct {
	a, b Expr
	Tok  scanner.Token
}

func (op simpleOperator) Precedence() int {
	return op.Tok.Precedence()
}

func (op simpleOperator) LeftHand() Expr {
	return op.a
}

func (op simpleOperator) RightHand() Expr {
	return op.b
}

func (op *simpleOperator) SetLeftHandExpr(a Expr) {
	op.a = a
}

func (op *simpleOperator) SetRightHandExpr(b Expr) {
	op.b = b
}

func (op *simpleOperator) Token() scanner.Token {
	return op.Tok
}

func (op *simpleOperator) eval(env *environment.Environment, fn func(a, b document.Value) (document.Value, error)) (document.Value, error) {
	if op.a == nil || op.b == nil {
		return NullLiteral, errors.New("missing operand")
	}

	va, err := op.a.Eval(env)
	if err != nil {
		return NullLiteral, err
	}

	vb, err := op.b.Eval(env)
	if err != nil {
		return NullLiteral, err
	}

	return fn(va, vb)
}

// Equal compares this expression with the other expression and returns
// true if they are equal.
func (op *simpleOperator) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	oop, ok := other.(Operator)
	if !ok {
		return false
	}

	return op.Tok == oop.Token() &&
		Equal(op.a, oop.LeftHand()) &&
		Equal(op.b, oop.RightHand())
}

func (op *simpleOperator) String() string {
	return stringutil.Sprintf("%v %v %v", op.a, op.Tok, op.b)
}

// An Operator is a binary expression that
// takes two operands and executes an operation on them.
type Operator interface {
	Expr

	Precedence() int
	LeftHand() Expr
	RightHand() Expr
	SetLeftHandExpr(Expr)
	SetRightHandExpr(Expr)
	Token() scanner.Token
}

// OperatorIsIndexCompatible returns whether the operator can be used to read from an index.
func OperatorIsIndexCompatible(op Operator) bool {
	switch op.Token() {
	case scanner.EQ, scanner.GT, scanner.GTE, scanner.LT, scanner.LTE, scanner.IN:
		return true
	}

	return false
}

type ConcatOperator struct {
	*simpleOperator
}

// Concat creates an expression that concatenates two text values together.
// It returns null if one of the values is not a text.
func Concat(a, b Expr) Expr {
	return &ConcatOperator{&simpleOperator{a, b, scanner.CONCAT}}
}

func (op *ConcatOperator) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		if a.Type() != document.TextValue || b.Type() != document.TextValue {
			return NullLiteral, nil
		}

		return document.NewTextValue(a.V().(string) + b.V().(string)), nil
	})
}
