package stream

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/sql/query/expr"
)

// A HashAggregateOperator consumes the given stream and outputs one value per group.
// It reads the _group variable from the environment to determine witch group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and aggregate them into one value.
type HashAggregateOperator struct {
	baseOperator
	Builders []expr.AggregatorBuilder
}

// HashAggregate consumes the incoming stream and outputs one value per group.
// It reads the _group variable from the environment to determine whitch group
// to assign each value. If no _group variable is available, it will assume all
// values are part of the same group and aggregate them into one value.
// HashAggregate assumes that the stream is not sorted per group and uses a hash map
// to group aggregates per _group value.
func HashAggregate(builders ...expr.AggregatorBuilder) *HashAggregateOperator {
	return &HashAggregateOperator{Builders: builders}
}

func (op *HashAggregateOperator) Iterate(in *expr.Environment, f func(out *expr.Environment) error) error {
	encGroup, err := newGroupEncoder()
	if err != nil {
		return err
	}

	// keep order of groups as they arrive to provide deterministic results.
	var encGroupNames []string

	// store a groupAggregator per group
	aggregators := make(map[string]*groupAggregator)

	// iterate over s and for each group, aggregate the incoming document
	err = op.Prev.Iterate(in, func(out *expr.Environment) error {
		// we extract the group name from the environment and encode it
		// to be used as a key to the aggregators map.
		groupName, err := encGroup(out)
		if err != nil {
			return err
		}

		// get the group aggregator from the map or create a new one.
		a, ok := aggregators[groupName]
		if !ok {
			a = newGroupAggregator(out, op.Builders)
			aggregators[groupName] = a
			encGroupNames = append(encGroupNames, groupName)
		}

		// call the aggregator for that group and aggregate the document.
		return a.Aggregate(out)
	})
	if err != nil {
		return err
	}

	// if s was empty, the aggregators map will be empty as well.
	// if so, we create one default group so that aggregators will
	// return their default initial value.
	// Ex: For `SELECT COUNT(*) FROM foo`, if `foo` is empty
	// we want the following result:
	// {"COUNT(*)": 0}
	if len(aggregators) == 0 {
		aggregators["_"] = newGroupAggregator(nil, op.Builders)
		encGroupNames = append(encGroupNames, "_")
	}

	// we loop over the groups in the order they arrived.
	for _, groupName := range encGroupNames {
		r := aggregators[groupName]
		e, err := r.Flush(in)
		if err != nil {
			return err
		}
		err = f(e)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *HashAggregateOperator) String() string {
	var sb strings.Builder

	for i, agg := range op.Builders {
		fmt.Fprintf(&sb, "%s", agg)
		if i+1 < len(op.Builders) {
			sb.WriteString(", ")
		}
	}

	return fmt.Sprintf("aggregate(%s)", sb.String())
}

// newGroupEncoder returns a function that encodes the _group environment variable using a document.ValueEncoder.
// If the _group variable doesn't exist, the group is set to null.
func newGroupEncoder() (func(env *expr.Environment) (string, error), error) {
	var b bytes.Buffer
	enc := document.NewValueEncoder(&b)
	nullValue := document.NewNullValue()
	err := enc.Encode(nullValue)
	if err != nil {
		return nil, err
	}
	nullGroupName := b.String()
	b.Reset()

	return func(env *expr.Environment) (string, error) {
		groupValue, ok := env.Get(document.NewPath(groupEnvKey))
		if !ok {
			return nullGroupName, nil
		}

		b.Reset()
		err := enc.Encode(groupValue)
		if err != nil {
			return "", err
		}

		return b.String(), nil
	}, nil
}

// a groupAggregator is an aggregator for a whole group of documents.
// It applies all the aggregators for each documents and returns a new document with the
// result of the aggregation.
type groupAggregator struct {
	group       document.Value
	groupExpr   string
	env         *expr.Environment
	aggregators []expr.Aggregator
}

func newGroupAggregator(outerEnv *expr.Environment, builders []expr.AggregatorBuilder) *groupAggregator {
	var env expr.Environment
	env.Outer = outerEnv

	newAggregators := make([]expr.Aggregator, len(builders))
	for i, b := range builders {
		newAggregators[i] = b.Aggregator()
	}

	ga := groupAggregator{
		env:         &env,
		aggregators: newAggregators,
	}

	var ok bool
	ga.group, ok = outerEnv.Get(document.NewPath(groupEnvKey))
	if !ok {
		ga.group = document.NewNullValue()
		return &ga
	}

	groupExprValue, _ := outerEnv.Get(document.NewPath(groupExprEnvKey))
	ga.groupExpr = groupExprValue.V.(string)

	return &ga
}

func (g *groupAggregator) Aggregate(env *expr.Environment) error {
	for _, agg := range g.aggregators {
		err := agg.Aggregate(env)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *groupAggregator) Flush(env *expr.Environment) (*expr.Environment, error) {
	fb := document.NewFieldBuffer()

	// add the current group to the document
	if g.groupExpr != "" {
		fb.Add(groupEnvKey, g.group)
	}

	for _, agg := range g.aggregators {
		v, err := agg.Eval(env)
		if err != nil {
			return nil, err
		}
		fb.Add(fmt.Sprintf("%s", agg), v)
	}

	var newEnv expr.Environment
	newEnv.Outer = env
	newEnv.SetDocument(fb)
	return &newEnv, nil
}
