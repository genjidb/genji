package stream

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

type DocumentsOperator struct {
	baseOperator
	Docs []document.Document
}

// Documents creates a DocumentsOperator that iterates over the given values.
func Documents(documents ...document.Document) *DocumentsOperator {
	return &DocumentsOperator{
		Docs: documents,
	}
}

func (op *DocumentsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, d := range op.Docs {
		newEnv.SetDocument(d)
		err := fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *DocumentsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("docs(")
	for i, d := range op.Docs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(d.(stringutil.Stringer).String())
	}
	sb.WriteString(")")

	return sb.String()
}

type ExprsOperator struct {
	baseOperator
	Exprs []expr.Expr
}

// Expressions creates an operator that iterates over the given expressions.
// Each expression must evaluate to a document.
func Expressions(exprs ...expr.Expr) *ExprsOperator {
	return &ExprsOperator{Exprs: exprs}
}

func (op *ExprsOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	for _, e := range op.Exprs {
		v, err := e.Eval(in)
		if err != nil {
			return err
		}
		if v.Type != document.DocumentValue {
			return ErrInvalidResult
		}

		newEnv.SetDocument(v.V().(document.Document))
		err = fn(&newEnv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (op *ExprsOperator) String() string {
	var sb strings.Builder

	sb.WriteString("exprs(")
	for i, e := range op.Exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e.(stringutil.Stringer).String())
	}
	sb.WriteByte(')')

	return sb.String()
}

// A SeqScanOperator iterates over the documents of a table.
type SeqScanOperator struct {
	baseOperator
	TableName string
	Reverse   bool
}

// SeqScan creates an iterator that iterates over each document of the given table.
func SeqScan(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName}
}

// SeqScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func SeqScanReverse(tableName string) *SeqScanOperator {
	return &SeqScanOperator{TableName: tableName, Reverse: true}
}

func (it *SeqScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	var iterator func(pivot document.Value, fn func(d document.Document) error) error
	if !it.Reverse {
		iterator = table.AscendGreaterOrEqual
	} else {
		iterator = table.DescendLessOrEqual
	}

	return iterator(document.Value{}, func(d document.Document) error {
		newEnv.SetDocument(d)
		return fn(&newEnv)
	})
}

func (it *SeqScanOperator) String() string {
	if !it.Reverse {
		return stringutil.Sprintf("seqScan(%s)", it.TableName)
	}
	return stringutil.Sprintf("seqScanReverse(%s)", it.TableName)
}

// A PkScanOperator iterates over the documents of a table.
type PkScanOperator struct {
	baseOperator
	TableName string
	Ranges    ValueRanges
	Reverse   bool
}

// PkScan creates an iterator that iterates over each document of the given table.
func PkScan(tableName string, ranges ...ValueRange) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges}
}

// PkScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func PkScanReverse(tableName string, ranges ...ValueRange) *PkScanOperator {
	return &PkScanOperator{TableName: tableName, Ranges: ranges, Reverse: true}
}

func (it *PkScanOperator) String() string {
	var s strings.Builder

	s.WriteString("pkScan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.TableName))
	if len(it.Ranges) > 0 {
		s.WriteString(", ")
		for i, r := range it.Ranges {
			s.WriteString(r.String())
			if i+1 < len(it.Ranges) {
				s.WriteString(", ")
			}
		}
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *PkScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	// if there are no ranges,  use a simpler and faster iteration function
	if len(it.Ranges) == 0 {
		s := SeqScan(it.TableName)
		s.Reverse = it.Reverse
		return s.Iterate(in, fn)
	}

	var newEnv environment.Environment
	newEnv.SetOuter(in)

	table, err := in.GetCatalog().GetTable(in.GetTx(), it.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.Encode(table, in)
	if err != nil {
		return err
	}

	var iterator func(pivot document.Value, fn func(d document.Document) error) error

	if !it.Reverse {
		iterator = table.AscendGreaterOrEqual
	} else {
		iterator = table.DescendLessOrEqual
	}

	for _, rng := range ranges {
		var start, end document.Value
		if !it.Reverse {
			start = rng.Min
			end = rng.Max
		} else {
			start = rng.Max
			end = rng.Min
		}

		var encEnd []byte
		if !end.Type.IsAny() && end.V() != nil {
			encEnd, err = table.EncodeValue(end)
			if err != nil {
				return err
			}
		}

		err = iterator(start, func(d document.Document) error {
			key := d.(document.Keyer).RawKey()

			if !rng.IsInRange(key) {
				// if we reached the end of our range, we can stop iterating.
				if encEnd == nil {
					return nil
				}
				cmp := bytes.Compare(key, encEnd)
				if !it.Reverse && cmp > 0 {
					return ErrStreamClosed
				}
				if it.Reverse && cmp < 0 {
					return ErrStreamClosed
				}
				return nil
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
		if err == ErrStreamClosed {
			err = nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// A IndexScanOperator iterates over the documents of an index.
type IndexScanOperator struct {
	baseOperator

	// IndexName references the index that will be used to perform the scan
	IndexName string
	// Ranges defines the boundaries of the scan, each corresponding to one value of the group of values
	// being indexed in the case of a composite index.
	Ranges IndexRanges
	// Reverse indicates the direction used to traverse the index.
	Reverse bool
}

// IndexScan creates an iterator that iterates over each document of the given table.
func IndexScan(name string, ranges ...IndexRange) *IndexScanOperator {
	return &IndexScanOperator{IndexName: name, Ranges: ranges}
}

// IndexScanReverse creates an iterator that iterates over each document of the given table in reverse order.
func IndexScanReverse(name string, ranges ...IndexRange) *IndexScanOperator {
	return &IndexScanOperator{IndexName: name, Ranges: ranges, Reverse: true}
}

func (it *IndexScanOperator) String() string {
	var s strings.Builder

	s.WriteString("indexScan")
	if it.Reverse {
		s.WriteString("Reverse")
	}

	s.WriteRune('(')

	s.WriteString(strconv.Quote(it.IndexName))
	if len(it.Ranges) > 0 {
		s.WriteString(", ")
		s.WriteString(it.Ranges.String())
	}

	s.WriteString(")")

	return s.String()
}

// Iterate over the documents of the table. Each document is stored in the environment
// that is passed to the fn function, using SetCurrentValue.
func (it *IndexScanOperator) Iterate(in *environment.Environment, fn func(out *environment.Environment) error) error {
	var newEnv environment.Environment
	newEnv.SetOuter(in)

	index, err := in.GetCatalog().GetIndex(in.GetTx(), it.IndexName)
	if err != nil {
		return err
	}

	table, err := in.GetCatalog().GetTable(in.GetTx(), index.Info.TableName)
	if err != nil {
		return err
	}

	ranges, err := it.Ranges.EncodeBuffer(index, table, in)
	if err != nil || len(ranges) != len(it.Ranges) {
		return err
	}

	var iterator func(pivot database.Pivot, fn func(val, key []byte) error) error

	if !it.Reverse {
		iterator = index.AscendGreaterOrEqual
	} else {
		iterator = index.DescendLessOrEqual
	}

	// if there are no ranges use a simpler and faster iteration function
	if len(ranges) == 0 {
		return iterator(nil, func(val, key []byte) error {
			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})
	}

	for _, rng := range ranges {
		var start, end *document.ValueBuffer
		if !it.Reverse {
			start = rng.Min
			end = rng.Max
		} else {
			start = rng.Max
			end = rng.Min
		}

		var encEnd []byte
		if end.Len() > 0 {
			encEnd, err = index.EncodeValueBuffer(end)
			if err != nil {
				return err
			}
		}

		var pivot database.Pivot
		if start != nil {
			pivot = start.Values
		}

		err = iterator(pivot, func(val, key []byte) error {
			if !rng.IsInRange(val) {
				// if we reached the end of our range, we can stop iterating.
				if encEnd == nil {
					return nil
				}

				cmp := bytes.Compare(val, encEnd)
				if !it.Reverse && cmp > 0 {
					return ErrStreamClosed
				}
				if it.Reverse && cmp < 0 {
					return ErrStreamClosed
				}
				return nil
			}

			d, err := table.GetDocument(key)
			if err != nil {
				return err
			}

			newEnv.SetDocument(d)
			return fn(&newEnv)
		})

		if err == ErrStreamClosed {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}
