package expr

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stringutil"
)

// A LiteralValue represents a literal value of any type defined by the value package.
type LiteralValue struct {
	Value document.Value
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (v LiteralValue) IsEqual(other Expr) bool {
	o, ok := other.(LiteralValue)
	if !ok {
		return false
	}
	ok, err := document.IsEqual(v.Value, o.Value)
	return ok && err == nil
}

// String implements the stringutil.Stringer interface.
func (v LiteralValue) String() string {
	return v.Value.String()
}

// Eval returns l. It implements the Expr interface.
func (v LiteralValue) Eval(*environment.Environment) (document.Value, error) {
	return document.Value(v.Value), nil
}

// LiteralExprList is a list of expressions.
type LiteralExprList []Expr

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (l LiteralExprList) IsEqual(other Expr) bool {
	o, ok := other.(LiteralExprList)
	if !ok {
		return false
	}
	if len(l) != len(o) {
		return false
	}

	for i := range l {
		if !Equal(l[i], o[i]) {
			return false
		}
	}

	return true
}

// String implements the stringutil.Stringer interface.
func (l LiteralExprList) String() string {
	var b strings.Builder

	b.WriteRune('[')
	for i, e := range l {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(stringutil.Sprintf("%v", e))
	}
	b.WriteRune(']')

	return b.String()
}

// Eval evaluates all the expressions and returns a literalValueList. It implements the Expr interface.
func (l LiteralExprList) Eval(env *environment.Environment) (document.Value, error) {
	var err error
	values := make([]document.Value, len(l))
	for i, e := range l {
		values[i], err = e.Eval(env)
		if err != nil {
			return NullLiteral, err
		}
	}

	return document.NewArrayValue(document.NewValueBuffer(values...)), nil
}

// KVPair associates an identifier with an expression.
type KVPair struct {
	K string
	V Expr
}

// String implements the stringutil.Stringer interface.
func (p KVPair) String() string {
	if stringutil.NeedsQuotes(p.K) {
		return stringutil.Sprintf("%q: %v", p.K, p.V)
	}
	return stringutil.Sprintf("%s: %v", p.K, p.V)
}

// KVPairs is a list of KVPair.
type KVPairs struct {
	Pairs          []KVPair
	SelfReferenced bool
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (kvp *KVPairs) IsEqual(other Expr) bool {
	o, ok := other.(*KVPairs)
	if !ok {
		return false
	}
	if kvp.SelfReferenced != o.SelfReferenced {
		return false
	}

	if len(kvp.Pairs) != len(o.Pairs) {
		return false
	}

	for i := range kvp.Pairs {
		if kvp.Pairs[i].K != o.Pairs[i].K {
			return false
		}
		if !Equal(kvp.Pairs[i].V, o.Pairs[i].V) {
			return false
		}
	}

	return true
}

// Eval turns a list of KVPairs into a document.
func (kvp *KVPairs) Eval(env *environment.Environment) (document.Value, error) {
	var fb document.FieldBuffer
	if kvp.SelfReferenced {
		if _, ok := env.GetDocument(); !ok {
			env.SetDocument(&fb)
		}
	}

	for _, kv := range kvp.Pairs {
		v, err := kv.V.Eval(env)
		if err != nil {
			return nil, err
		}

		fb.Add(kv.K, v)
	}

	return document.NewDocumentValue(&fb), nil
}

// String implements the stringutil.Stringer interface.
func (kvp *KVPairs) String() string {
	var b strings.Builder

	b.WriteRune('{')
	for i, p := range kvp.Pairs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(stringutil.Sprintf("%s", p))
	}
	b.WriteRune('}')

	return b.String()
}
