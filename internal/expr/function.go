package expr

import (
	"errors"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Function is an expression that execute some arbitrary code.
type Function interface {
	Expr

	// Returns the list of parameters this function has received.
	Params() []Expr
}

// Functions represents a map of builtin SQL functions.
type Functions struct {
	m map[string]func(args ...Expr) (Expr, error)
}

// BuiltinFunctions returns default map of builtin functions.
func BuiltinFunctions() map[string]func(args ...Expr) (Expr, error) {
	return map[string]func(args ...Expr) (Expr, error){
		"pk": func(args ...Expr) (Expr, error) {
			if len(args) != 0 {
				return nil, stringutil.Errorf("pk() takes no arguments")
			}
			return new(PKFunc), nil
		},
		"count": func(args ...Expr) (Expr, error) {
			if len(args) != 1 {
				return nil, stringutil.Errorf("COUNT() takes 1 argument")
			}
			return &CountFunc{Expr: args[0]}, nil
		},
		"min": func(args ...Expr) (Expr, error) {
			if len(args) != 1 {
				return nil, stringutil.Errorf("MIN() takes 1 argument")
			}
			return &MinFunc{Expr: args[0]}, nil
		},
		"max": func(args ...Expr) (Expr, error) {
			if len(args) != 1 {
				return nil, stringutil.Errorf("MAX() takes 1 argument")
			}
			return &MaxFunc{Expr: args[0]}, nil
		},
		"sum": func(args ...Expr) (Expr, error) {
			if len(args) != 1 {
				return nil, stringutil.Errorf("SUM() takes 1 argument")
			}
			return &SumFunc{Expr: args[0]}, nil
		},
		"avg": func(args ...Expr) (Expr, error) {
			if len(args) != 1 {
				return nil, stringutil.Errorf("AVG() takes 1 argument")
			}
			return &AvgFunc{Expr: args[0]}, nil
		},
	}
}

func NewFunctions() Functions {
	return Functions{
		m: BuiltinFunctions(),
	}
}

// AddFunc adds function to the map.
func (f Functions) AddFunc(name string, fn func(args ...Expr) (Expr, error)) {
	f.m[name] = fn
}

// GetFunc return a function expression by name.
func (f Functions) GetFunc(name string, args ...Expr) (Expr, error) {
	fn, ok := f.m[strings.ToLower(name)]
	if !ok {
		return nil, stringutil.Errorf("no such function: %q", name)
	}

	return fn(args...)
}

// A Aggregator is an expression that aggregates documents into one result.
type Aggregator interface {
	Expr

	Aggregate(env *environment.Environment) error
}

// An AggregatorBuilder is a type that can create aggregators.
type AggregatorBuilder interface {
	Expr

	Aggregator() Aggregator
}

// PKFunc represents the pk() function.
// It returns the primary key of the current document.
type PKFunc struct{}

// Eval returns the primary key of the current document.
func (k *PKFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return NullLiteral, nil
	}

	keyer, ok := d.(document.Keyer)
	if !ok {
		return NullLiteral, nil
	}

	v, err := keyer.Key()
	return v, err
}

func (*PKFunc) Params() []Expr { return nil }

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (k *PKFunc) IsEqual(other Expr) bool {
	_, ok := other.(*PKFunc)
	return ok
}

func (k *PKFunc) String() string {
	return "pk()"
}

// CastFunc represents the CAST expression.
type CastFunc struct {
	Expr   Expr
	CastAs document.ValueType
}

// Eval returns the primary key of the current document.
func (c CastFunc) Eval(env *environment.Environment) (document.Value, error) {
	v, err := c.Expr.Eval(env)
	if err != nil {
		return v, err
	}

	return v.CastAs(c.CastAs)
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c CastFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(CastFunc)
	if !ok {
		return false
	}

	if c.CastAs != o.CastAs {
		return false
	}

	if c.Expr != nil {
		return Equal(c.Expr, o.Expr)
	}

	return o.Expr != nil
}

func (c CastFunc) Params() []Expr { return []Expr{c.Expr} }

func (c CastFunc) String() string {
	return stringutil.Sprintf("CAST(%v AS %v)", c.Expr, c.CastAs)
}

var _ AggregatorBuilder = (*CountFunc)(nil)

// CountFunc is the COUNT aggregator function. It counts the number of documents
// in a stream.
type CountFunc struct {
	Expr     Expr
	Wildcard bool
	Count    int64
}

func (c *CountFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function COUNT()")
	}

	return d.GetByField(c.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (c *CountFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*CountFunc)
	if !ok {
		return false
	}

	if c.Wildcard && o.Wildcard {
		return c.Expr == nil && o.Expr == nil
	}

	return Equal(c.Expr, o.Expr)
}

func (c *CountFunc) Params() []Expr { return []Expr{c.Expr} }

func (c *CountFunc) String() string {
	if c.Wildcard {
		return "COUNT(*)"
	}

	return stringutil.Sprintf("COUNT(%v)", c.Expr)
}

// Aggregator returns a CountAggregator. It implements the AggregatorBuilder interface.
func (c *CountFunc) Aggregator() Aggregator {
	return &CountAggregator{
		Fn: c,
	}
}

// CountAggregator is an aggregator that counts non-null expressions.
type CountAggregator struct {
	Fn    *CountFunc
	Count int64
}

// Aggregate increments the counter if the count expression evaluates to a non-null value.
func (c *CountAggregator) Aggregate(env *environment.Environment) error {
	if c.Fn.Wildcard {
		c.Count++
		return nil
	}

	v, err := c.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v != NullLiteral {
		c.Count++
	}

	return nil
}

// Eval returns the result of the aggregation as an integer.
func (c *CountAggregator) Eval(env *environment.Environment) (document.Value, error) {
	return document.NewIntegerValue(c.Count), nil
}

func (c *CountAggregator) String() string {
	return c.Fn.String()
}

// MinFunc is the MIN aggregator function.
type MinFunc struct {
	Expr Expr
}

// Eval extracts the min value from the given document and returns it.
func (m *MinFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function MIN()")
	}

	return d.GetByField(m.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *MinFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*MinFunc)
	if !ok {
		return false
	}

	return Equal(m.Expr, o.Expr)
}

func (m *MinFunc) Params() []Expr { return []Expr{m.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *MinFunc) String() string {
	return stringutil.Sprintf("MIN(%v)", m.Expr)
}

// Aggregator returns a MinAggregator. It implements the AggregatorBuilder interface.
func (m *MinFunc) Aggregator() Aggregator {
	return &MinAggregator{
		Fn: m,
	}
}

// MinAggregator is an aggregator that returns the minimum non-null value.
type MinAggregator struct {
	Fn  *MinFunc
	Min document.Value
}

// Aggregate stores the minimum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MinAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == NullLiteral {
		return nil
	}

	if m.Min.Type() == 0 {
		m.Min = v
		return nil
	}

	if m.Min.Type() == v.Type() || m.Min.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := m.Min.IsGreaterThan(v)
		if err != nil {
			return err
		}
		if ok {
			m.Min = v
		}

		return nil
	}

	if m.Min.Type() > v.Type() {
		m.Min = v
	}

	return nil
}

// Eval return the minimum value.
func (m *MinAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if m.Min.Type() == 0 {
		return document.NewNullValue(), nil
	}
	return m.Min, nil
}

func (m *MinAggregator) String() string {
	return m.Fn.String()
}

// MaxFunc is the MAX aggregator function.
type MaxFunc struct {
	Expr Expr
}

// Eval extracts the max value from the given document and returns it.
func (m *MaxFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function MAX()")
	}

	return d.GetByField(m.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (m *MaxFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*MaxFunc)
	if !ok {
		return false
	}

	return Equal(m.Expr, o.Expr)
}

func (m *MaxFunc) Params() []Expr { return []Expr{m.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (m *MaxFunc) String() string {
	return stringutil.Sprintf("MAX(%v)", m.Expr)
}

// Aggregator returns a MaxAggregator. It implements the AggregatorBuilder interface.
func (m *MaxFunc) Aggregator() Aggregator {
	return &MaxAggregator{
		Fn: m,
	}
}

// MaxAggregator is an aggregator that returns the minimum non-null value.
type MaxAggregator struct {
	Fn  *MaxFunc
	Max document.Value
}

// Aggregate stores the maximum value. Values are compared based on their types,
// then if the type is equal their value is compared. Numbers are considered of the same type.
func (m *MaxAggregator) Aggregate(env *environment.Environment) error {
	v, err := m.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v == NullLiteral {
		return nil
	}

	if m.Max.Type() == 0 {
		m.Max = v
		return nil
	}

	if m.Max.Type() == v.Type() || m.Max.Type().IsNumber() && v.Type().IsNumber() {
		ok, err := m.Max.IsLesserThan(v)
		if err != nil {
			return err
		}
		if ok {
			m.Max = v
		}

		return nil
	}

	if m.Max.Type() < v.Type() {
		m.Max = v
	}

	return nil
}

// Eval return the maximum value.
func (m *MaxAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if m.Max.Type() == 0 {
		return document.NewNullValue(), nil
	}

	return m.Max, nil
}

func (m *MaxAggregator) String() string {
	return m.Fn.String()
}

// SumFunc is the SUM aggregator function.
type SumFunc struct {
	Expr Expr
}

// Eval extracts the sum value from the given document and returns it.
func (s *SumFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function SUM()")
	}

	return d.GetByField(s.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (s *SumFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*SumFunc)
	if !ok {
		return false
	}

	return Equal(s.Expr, o.Expr)
}

func (s *SumFunc) Params() []Expr { return []Expr{s.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the count expression.
func (s *SumFunc) String() string {
	return stringutil.Sprintf("SUM(%v)", s.Expr)
}

// Aggregator returns a SumFunc. It implements the AggregatorBuilder interface.
func (s *SumFunc) Aggregator() Aggregator {
	return &SumAggregator{
		Fn: s,
	}
}

// SumAggregator is an aggregator that returns the minimum non-null value.
type SumAggregator struct {
	Fn   *SumFunc
	SumI *int64
	SumF *float64
}

// Aggregate stores the sum of all non-NULL numeric values in the group.
// The result is an integer value if all summed values are integers.
// If any of the value is a double, the returned result will be a double.
func (s *SumAggregator) Aggregate(env *environment.Environment) error {
	v, err := s.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}
	if v.Type() != document.IntegerValue && v.Type() != document.DoubleValue {
		return nil
	}

	if s.SumF != nil {
		if v.Type() == document.IntegerValue {
			*s.SumF += float64(v.V().(int64))
		} else {
			*s.SumF += float64(v.V().(float64))
		}

		return nil
	}

	if v.Type() == document.DoubleValue {
		var sumF float64
		if s.SumI != nil {
			sumF = float64(*s.SumI)
		}
		s.SumF = &sumF
		*s.SumF += float64(v.V().(float64))

		return nil
	}

	if s.SumI == nil {
		var sumI int64
		s.SumI = &sumI
	}

	*s.SumI += v.V().(int64)
	return nil
}

// Eval return the aggregated sum.
func (s *SumAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if s.SumF != nil {
		return document.NewDoubleValue(*s.SumF), nil
	}
	if s.SumI != nil {
		return document.NewIntegerValue(*s.SumI), nil
	}

	return document.NewNullValue(), nil
}

func (s *SumAggregator) String() string {
	return s.Fn.String()
}

// AvgFunc is the AVG aggregator function.
type AvgFunc struct {
	Expr Expr
}

// Eval extracts the average value from the given document and returns it.
func (s *AvgFunc) Eval(env *environment.Environment) (document.Value, error) {
	d, ok := env.GetDocument()
	if !ok {
		return document.Value{}, errors.New("misuse of aggregation function AVG()")
	}

	return d.GetByField(s.String())
}

// IsEqual compares this expression with the other expression and returns
// true if they are equal.
func (s *AvgFunc) IsEqual(other Expr) bool {
	if other == nil {
		return false
	}

	o, ok := other.(*AvgFunc)
	if !ok {
		return false
	}

	return Equal(s.Expr, o.Expr)
}

func (s *AvgFunc) Params() []Expr { return []Expr{s.Expr} }

// String returns the alias if non-zero, otherwise it returns a string representation
// of the average expression.
func (s *AvgFunc) String() string {
	return stringutil.Sprintf("AVG(%v)", s.Expr)
}

// Aggregator returns a AvgFunc. It implements the AggregatorBuilder interface.
func (s *AvgFunc) Aggregator() Aggregator {
	return &AvgAggregator{
		Fn: s,
	}
}

// AvgAggregator is an aggregator that returns the average non-null value.
type AvgAggregator struct {
	Fn      *AvgFunc
	Avg     float64
	Counter int64
}

// Aggregate stores the average value of all non-NULL numeric values in the group.
func (s *AvgAggregator) Aggregate(env *environment.Environment) error {
	v, err := s.Fn.Expr.Eval(env)
	if err != nil && err != document.ErrFieldNotFound {
		return err
	}

	switch v.Type() {
	case document.IntegerValue:
		s.Avg += float64(v.V().(int64))
	case document.DoubleValue:
		s.Avg += v.V().(float64)
	default:
		return nil
	}
	s.Counter++

	return nil
}

// Eval returns the aggregated average as a double.
func (s *AvgAggregator) Eval(env *environment.Environment) (document.Value, error) {
	if s.Counter == 0 {
		return document.NewDoubleValue(0), nil
	}

	return document.NewDoubleValue(s.Avg / float64(s.Counter)), nil
}

func (s *AvgAggregator) String() string {
	return s.Fn.String()
}
