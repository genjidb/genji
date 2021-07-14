package stream

import (
	"bytes"
	"container/heap"
	"errors"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

const (
	groupEnvKey     = "_group"
	groupExprEnvKey = "_group_expr"
	accEnvKey       = "_acc"
)

// ErrInvalidResult is returned when an expression supposed to evaluate to a document
// returns something else.
var ErrInvalidResult = errors.New("expression must evaluate to a document")

// An Operator is used to modify a stream.
// It takes an environment containing the current value as well as any other metadata
// created by other operatorsand returns a new environment which will be passed to the next operator.
// If it returns a nil environment, the env will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the Op closure
// unless the nature of the operator prevents that.
type Operator interface {
	Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error
	SetPrev(prev Operator)
	SetNext(next Operator)
	GetNext() Operator
	GetPrev() Operator
	String() string
}

// An OperatorFunc is the function that will receive each value of the stream.
type OperatorFunc func(func(env *environment.Environment) error) error

func Pipe(ops ...Operator) Operator {
	for i := len(ops) - 1; i > 0; i-- {
		ops[i].SetPrev(ops[i-1])
		ops[i-1].SetNext(ops[i])
	}

	return ops[len(ops)-1]
}

type baseOperator struct {
	Prev Operator
	Next Operator
}

func (op *baseOperator) SetPrev(o Operator) {
	op.Prev = o
}

func (op *baseOperator) SetNext(o Operator) {
	op.Next = o
}

func (op *baseOperator) GetPrev() Operator {
	return op.Prev
}

func (op *baseOperator) GetNext() Operator {
	return op.Next
}

// A MapOperator applies an expression on each value of the stream and returns a new value.
type MapOperator struct {
	baseOperator
	E expr.Expr
}

// Map evaluates e on each value of the stream and outputs the result.
func Map(e expr.Expr) *MapOperator {
	return &MapOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *MapOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		if v.Type() != document.DocumentValue {
			return ErrInvalidResult
		}

		newEnv.SetDocument(v.V().(document.Document))
		newEnv.SetOuter(out)
		return f(&newEnv)
	})
}

func (op *MapOperator) String() string {
	return stringutil.Sprintf("map(%s)", op.E)
}

// A FilterOperator filters values based on a given expression.
type FilterOperator struct {
	baseOperator
	E expr.Expr
}

// Filter evaluates e for each incoming value and filters any value whose result is not truthy.
func Filter(e expr.Expr) *FilterOperator {
	return &FilterOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *FilterOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		ok, err := document.IsTruthy(v)
		if err != nil || !ok {
			return err
		}

		return f(out)
	})
}

func (op *FilterOperator) String() string {
	return stringutil.Sprintf("filter(%s)", op.E)
}

// A TakeOperator closes the stream after a certain number of values.
type TakeOperator struct {
	baseOperator
	N int64
}

// Take closes the stream after n values have passed through the operator.
func Take(n int64) *TakeOperator {
	return &TakeOperator{N: n}
}

// Iterate implements the Operator interface.
func (op *TakeOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var count int64
	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if count < op.N {
			count++
			return f(out)
		}

		return ErrStreamClosed
	})
}

func (op *TakeOperator) String() string {
	return stringutil.Sprintf("take(%d)", op.N)
}

// A SkipOperator skips the n first values of the stream.
type SkipOperator struct {
	baseOperator
	N int64
}

// Skip ignores the first n values of the stream.
func Skip(n int64) *SkipOperator {
	return &SkipOperator{N: n}
}

// Iterate implements the Operator interface.
func (op *SkipOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var skipped int64

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		if skipped < op.N {
			skipped++
			return nil
		}

		return f(out)
	})
}

func (op *SkipOperator) String() string {
	return stringutil.Sprintf("skip(%d)", op.N)
}

// A GroupByOperator applies an expression on each value of the stream and stores the result in the _group
// variable in the output stream.
type GroupByOperator struct {
	baseOperator
	E expr.Expr
}

// GroupBy applies e on each value of the stream and stores the result in the _group
// variable in the output stream.
func GroupBy(e expr.Expr) *GroupByOperator {
	return &GroupByOperator{E: e}
}

// Iterate implements the Operator interface.
func (op *GroupByOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		v, err := op.E.Eval(out)
		if err != nil {
			return err
		}

		newEnv.Set(groupEnvKey, v)
		newEnv.Set(groupExprEnvKey, document.NewTextValue(stringutil.Sprintf("%s", op.E)))
		newEnv.SetOuter(out)
		return f(&newEnv)
	})
}

func (op *GroupByOperator) String() string {
	return stringutil.Sprintf("groupBy(%s)", op.E)
}

// A SortOperator consumes every value of the stream and outputs them in order.
type SortOperator struct {
	baseOperator
	Expr expr.Expr
	Desc bool
}

// Sort consumes every value of the stream and outputs them in order.
// It operates a partial sort on the iterator using a heap.
// This ensures a O(k+n log n) time complexity, where k is the sum of
// Take() + Skip() operators, if provided, otherwise k = n.
// If the sorting is in ascending order, a min-heap will be used
// otherwise a max-heap will be used instead.
// Once the heap is filled entirely with the content of the incoming stream, a stream is returned.
// During iteration, the stream will pop the k-smallest or k-largest elements, depending on
// the chosen sorting order (ASC or DESC).
// This function is not memory efficient as it is loading the entire stream in memory before
// returning the k-smallest or k-largest elements.
func Sort(e expr.Expr) *SortOperator {
	return &SortOperator{Expr: e}
}

// SortReverse does the same as Sort but in descending order.
func SortReverse(e expr.Expr) *SortOperator {
	return &SortOperator{Expr: e, Desc: true}
}

func (op *SortOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	h, err := op.sortStream(op.Prev, in)
	if err != nil {
		return err
	}

	for h.Len() > 0 {
		node := heap.Pop(h).(heapNode)
		err := f(node.data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *SortOperator) sortStream(prev Operator, in *environment.Environment) (heap.Interface, error) {
	var h heap.Interface
	if op.Desc {
		h = new(maxHeap)
	} else {
		h = new(minHeap)
	}

	heap.Init(h)

	getValue := op.Expr.Eval
	if p, ok := op.Expr.(expr.Path); ok {
		getValue = func(env *environment.Environment) (document.Value, error) {
			for env != nil {
				d, ok := env.GetDocument()
				if !ok {
					env = env.GetOuter()
					continue
				}

				v, err := document.Path(p).GetValueFromDocument(d)
				if err == document.ErrFieldNotFound {
					env = env.GetOuter()
					continue
				}
				return v, err
			}

			return document.NewNullValue(), nil
		}
	}

	return h, prev.Iterate(in, func(env *environment.Environment) error {
		sortV, err := getValue(env)
		if err != nil {
			return err
		}

		// We need to make sure sort behaviour
		// is the same with or without indexes.
		// To achieve that, the value must be encoded using the same method
		// as what the index package would do.
		var buf bytes.Buffer

		err = document.NewValueEncoder(&buf).Encode(sortV)
		if err != nil {
			return err
		}

		node := heapNode{
			value: buf.Bytes(),
		}
		e, err := env.Clone()
		if err != nil {
			return err
		}
		node.data = e

		heap.Push(h, node)

		return nil
	})
}

func (op *SortOperator) String() string {
	if op.Desc {
		return stringutil.Sprintf("sortReverse(%s)", op.Expr)
	}

	return stringutil.Sprintf("sort(%s)", op.Expr)
}

type heapNode struct {
	value []byte
	data  *environment.Environment
}

type minHeap []heapNode

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return bytes.Compare(h[i].value, h[j].value) < 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapNode))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type maxHeap struct {
	minHeap
}

func (h maxHeap) Less(i, j int) bool {
	return bytes.Compare(h.minHeap[i].value, h.minHeap[j].value) > 0
}

// A TableInsertOperator inserts incoming documents to the table.
type TableInsertOperator struct {
	baseOperator
	Name       string
	OnConflict database.OnInsertConflictAction
}

// TableInsert inserts incoming documents to the table.
func TableInsert(tableName string, onConflict database.OnInsertConflictAction) *TableInsertOperator {
	return &TableInsertOperator{Name: tableName, OnConflict: onConflict}
}

// Iterate implements the Operator interface.
func (op *TableInsertOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var newEnv environment.Environment

	var table *database.Table
	return op.Prev.Iterate(in, func(env *environment.Environment) error {
		d, ok := env.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var err error
		if table == nil {
			table, err = env.GetCatalog().GetTable(env.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		d, err = table.InsertWithConflictResolution(d, op.OnConflict)
		if err != nil {
			return err
		}
		newEnv.SetDocument(d)

		newEnv.SetOuter(env)
		return f(&newEnv)
	})
}

func (op *TableInsertOperator) String() string {
	if op.OnConflict != nil {
		return stringutil.Sprintf("tableInsert('%s', onConflictDoNothing)", op.Name)
	}

	return stringutil.Sprintf("tableInsert('%s')", op.Name)
}

// A TableReplaceOperator replaces documents in the table
type TableReplaceOperator struct {
	baseOperator
	Name string
}

// TableReplace replaces documents in the table. Incoming documents must implement the document.Keyer interface.
func TableReplace(tableName string) *TableReplaceOperator {
	return &TableReplaceOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *TableReplaceOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		ker, ok := d.(document.Keyer)
		if !ok {
			return errors.New("missing key")
		}

		k := ker.RawKey()
		if k == nil {
			return errors.New("missing key")
		}

		_, err := table.Replace(ker.RawKey(), d)
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		return f(&newEnv)
	})
}

func (op *TableReplaceOperator) String() string {
	return stringutil.Sprintf("tableReplace('%s')", op.Name)
}

// A TableDeleteOperator replaces documents in the table
type TableDeleteOperator struct {
	baseOperator
	Name string
}

// TableDelete deletes documents from the table. Incoming documents must implement the document.Keyer interface.
func TableDelete(tableName string) *TableDeleteOperator {
	return &TableDeleteOperator{Name: tableName}
}

// Iterate implements the Operator interface.
func (op *TableDeleteOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var table *database.Table
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		if table == nil {
			var err error
			table, err = out.GetCatalog().GetTable(out.GetTx(), op.Name)
			if err != nil {
				return err
			}
		}

		ker, ok := d.(document.Keyer)
		if !ok {
			return errors.New("missing key")
		}

		k := ker.RawKey()
		if k == nil {
			return errors.New("missing key")
		}

		err := table.Delete(ker.RawKey())
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		return f(&newEnv)
	})
}

func (op *TableDeleteOperator) String() string {
	return stringutil.Sprintf("tableDelete('%s')", op.Name)
}

// A DistinctOperator filters duplicate documents.
type DistinctOperator struct {
	baseOperator
}

// Distinct filters duplicate documents based on one or more expressions.
func Distinct() *DistinctOperator {
	return &DistinctOperator{}
}

// Iterate implements the Operator interface.
func (op *DistinctOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var buf bytes.Buffer
	enc := document.NewValueEncoder(&buf)
	m := make(map[string]struct{})

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		buf.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		fields, err := document.Fields(d)
		if err != nil {
			return err
		}

		for _, field := range fields {
			value, err := d.GetByField(field)
			if err != nil {
				return err
			}

			err = enc.Encode(value)
			if err != nil {
				return err
			}
		}

		_, ok = m[string(buf.Bytes())]
		// if value already exists, filter it out
		if ok {
			return nil
		}

		m[buf.String()] = struct{}{}

		return f(out)
	})
}

func (op *DistinctOperator) String() string {
	return "distinct()"
}

// A SetOperator filters duplicate documents.
type SetOperator struct {
	baseOperator
	Path document.Path
	E    expr.Expr
}

// Set filters duplicate documents based on one or more expressions.
func Set(path document.Path, e expr.Expr) *SetOperator {
	return &SetOperator{
		Path: path,
		E:    e,
	}
}

// Iterate implements the Operator interface.
func (op *SetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		v, err := op.E.Eval(out)
		if err != nil && err != document.ErrFieldNotFound {
			return err
		}

		fb.Reset()
		err = fb.ScanDocument(d)
		if err != nil {
			return err
		}

		err = fb.Set(op.Path, v)
		if err == document.ErrFieldNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *SetOperator) String() string {
	return stringutil.Sprintf("set(%s, %s)", op.Path, op.E)
}

// A UnsetOperator filters duplicate documents.
type UnsetOperator struct {
	baseOperator
	Field string
}

// Unset filters duplicate documents based on one or more expressions.
func Unset(field string) *UnsetOperator {
	return &UnsetOperator{
		Field: field,
	}
}

// Iterate implements the Operator interface.
func (op *UnsetOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		_, err := d.GetByField(op.Field)
		if err != nil {
			if err != document.ErrFieldNotFound {
				return err
			}

			return f(out)
		}

		err = fb.ScanDocument(d)
		if err != nil {
			return err
		}

		err = fb.Delete(document.NewPath(op.Field))
		if err != nil {
			return err
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *UnsetOperator) String() string {
	return stringutil.Sprintf("unset(%s)", op.Field)
}

// An IterRenameOperator iterates over all fields of the incoming document in order and renames them.
type IterRenameOperator struct {
	baseOperator
	FieldNames []string
}

// IterRename iterates over all fields of the incoming document in order and renames them.
// If the number of fields of the incoming document doesn't match the number of expected fields,
// it returns an error.
func IterRename(fieldNames ...string) *IterRenameOperator {
	return &IterRenameOperator{
		FieldNames: fieldNames,
	}
}

// Iterate implements the Operator interface.
func (op *IterRenameOperator) Iterate(in *environment.Environment, f func(out *environment.Environment) error) error {
	var fb document.FieldBuffer
	var newEnv environment.Environment

	return op.Prev.Iterate(in, func(out *environment.Environment) error {
		fb.Reset()

		d, ok := out.GetDocument()
		if !ok {
			return errors.New("missing document")
		}

		var i int
		err := d.Iterate(func(field string, value document.Value) error {
			// if there are too many fields in the incoming document
			if i >= len(op.FieldNames) {
				n, err := document.Length(d)
				if err != nil {
					return err
				}
				return stringutil.Errorf("%d values for %d fields", n, len(op.FieldNames))
			}

			fb.Add(op.FieldNames[i], value)
			i++
			return nil
		})
		if err != nil {
			return err
		}

		// if there are too few fields in the incoming document
		if i < len(op.FieldNames) {
			n, err := document.Length(d)
			if err != nil {
				return err
			}
			return stringutil.Errorf("%d values for %d fields", n, len(op.FieldNames))
		}

		newEnv.SetOuter(out)
		newEnv.SetDocument(&fb)

		return f(&newEnv)
	})
}

func (op *IterRenameOperator) String() string {
	return stringutil.Sprintf("iterRename(%s)", strings.Join(op.FieldNames, ", "))
}
