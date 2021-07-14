package expr

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr/glob"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stringutil"
)

func like(pattern, text string) bool {
	return glob.MatchLike(pattern, text)
}

type LikeOperator struct {
	*simpleOperator
}

// Like creates an expression that evaluates to the result of a LIKE b.
func Like(a, b Expr) Expr {
	return &LikeOperator{&simpleOperator{a, b, scanner.LIKE}}
}

func (op *LikeOperator) Eval(env *environment.Environment) (document.Value, error) {
	return op.simpleOperator.eval(env, func(a, b document.Value) (document.Value, error) {
		if a.Type != document.TextValue || b.Type != document.TextValue {
			return NullLiteral, nil
		}

		if like(b.V().(string), a.V().(string)) {
			return TrueLiteral, nil
		}

		return FalseLiteral, nil
	})
}

type NotLikeOperator struct {
	LikeOperator
}

// NotLike creates an expression that evaluates to the result of a NOT LIKE b.
func NotLike(a, b Expr) Expr {
	return &NotLikeOperator{LikeOperator{&simpleOperator{a, b, scanner.LIKE}}}
}

func (op *NotLikeOperator) Eval(env *environment.Environment) (document.Value, error) {
	return invertBoolResult(op.LikeOperator.Eval)(env)
}

func (op *NotLikeOperator) String() string {
	return stringutil.Sprintf("%v NOT LIKE %v", op.a, op.b)
}
