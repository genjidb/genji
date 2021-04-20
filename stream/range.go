package stream

import (
	"bytes"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/expr"
	"github.com/genjidb/genji/stringutil"
)

type Costable interface {
	Cost() int
}

type ValueRange struct {
	Min, Max document.Value
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool

	encodedMin, encodedMax []byte
	rangeType              document.ValueType
}

func (r *ValueRange) encode(encoder ValueEncoder, env *expr.Environment) error {
	var err error

	// first we evaluate Min and Max
	if !r.Min.Type.IsZero() {
		r.encodedMin, err = encoder.EncodeValue(r.Min)
		if err != nil {
			return err
		}
		r.rangeType = r.Min.Type
	}
	if !r.Max.Type.IsZero() {
		r.encodedMax, err = encoder.EncodeValue(r.Max)
		if err != nil {
			return err
		}
		if !r.rangeType.IsZero() && r.rangeType != r.Max.Type {
			panic("range contain values of different types")
		}

		r.rangeType = r.Max.Type
	}

	// ensure boundaries are typed
	if r.Min.Type.IsZero() {
		r.Min.Type = r.rangeType
	}
	if r.Max.Type.IsZero() {
		r.Max.Type = r.rangeType
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	return nil
}

func (r *ValueRange) String() string {
	if r.Exact {
		return stringutil.Sprintf("%v", r.Min)
	}

	if r.Min.Type.IsZero() {
		r.Min = document.NewIntegerValue(-1)
	}
	if r.Max.Type.IsZero() {
		r.Max = document.NewIntegerValue(-1)
	}

	if r.Exclusive {
		return stringutil.Sprintf("[%v, %v, true]", r.Min, r.Max)
	}

	return stringutil.Sprintf("[%v, %v]", r.Min, r.Max)
}

func (r *ValueRange) IsEqual(other *ValueRange) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.rangeType != other.rangeType {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if r.Min.Type != other.Min.Type {
		return false
	}
	ok, err := r.Min.IsEqual(other.Min)
	if err != nil || !ok {
		return false
	}

	if r.Max.Type != other.Max.Type {
		return false
	}
	ok, err = r.Max.IsEqual(other.Max)
	if err != nil || !ok {
		return false
	}

	return true
}

type ValueRanges []ValueRange

// Append rng to r and return the new slice.
// Duplicate ranges are ignored.
func (r ValueRanges) Append(rng ValueRange) ValueRanges {
	// ensure we don't keep duplicate ranges
	isDuplicate := false
	for _, e := range r {
		if e.IsEqual(&rng) {
			isDuplicate = true
			break
		}
	}

	if isDuplicate {
		return r
	}

	return append(r, rng)
}

type ValueEncoder interface {
	EncodeValue(v document.Value) ([]byte, error)
}

// Encode each range using the given value encoder.
func (r ValueRanges) Encode(encoder ValueEncoder, env *expr.Environment) error {
	for i := range r {
		err := r[i].encode(encoder, env)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r ValueRanges) String() string {
	var sb strings.Builder

	for i, rr := range r {
		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(rr.String())
	}

	return sb.String()
}

// Cost is a best effort function to determine the cost of
// a range lookup.
func (r ValueRanges) Cost() int {
	var cost int

	for _, rng := range r {
		// if we are looking for an exact value
		// increment by 1
		if rng.Exact {
			cost++
			continue
		}

		// if there are two boundaries, increment by 50
		if !rng.Min.Type.IsZero() && !rng.Max.Type.IsZero() {
			cost += 50
		}

		// if there is only one boundary, increment by 100
		if (!rng.Min.Type.IsZero() && rng.Max.Type.IsZero()) || (rng.Min.Type.IsZero() && !rng.Max.Type.IsZero()) {
			cost += 100
			continue
		}

		// if there are no boundaries, increment by 200
		cost += 200
	}

	return cost
}

func (r *ValueRange) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if r.encodedMin != nil {
		cmpMin = bytes.Compare(value, r.encodedMin)
	}

	// if exact is true the value has to be equal to the lower bound.
	if r.Exact {
		return cmpMin == 0
	}

	// if exclusive and the value is equal to the lower bound
	// we can ignore it
	if r.Exclusive && cmpMin == 0 {
		return false
	}

	// the value is bigger than the lower bound,
	// see if it matches the upper bound.
	if r.encodedMax != nil {
		cmpMax = bytes.Compare(value, r.encodedMax)
	}

	// if boundaries are strict, ignore values equal to the max
	if r.Exclusive && cmpMax == 0 {
		return false
	}

	return cmpMax <= 0
}

type IndexRange struct {
	Min, Max *document.ValueBuffer
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool

	// Arity represents the range arity in the case of comparing the range
	// to a composite index. With IndexArityMax, it enables to deal with the
	// cases of a composite range specifying boundaries partially, ie:
	// - Index on (a, b, c)
	// - Range is defining a max only for a and b
	// Then Arity is set to 2 and IndexArityMax is set to 3
	//
	// On
	// This field is subject to change when the support for composite index is added
	// to the query planner in an ulterior pull-request.
	Arity int

	// IndexArityMax represents the underlying Index arity.
	//
	// This field is subject to change when the support for composite index is added
	// to the query planner in an ulterior pull-request.
	IndexArityMax          int
	encodedMin, encodedMax []byte
	rangeTypes             []document.ValueType
}

func (r *IndexRange) encode(encoder ValueBufferEncoder, env *expr.Environment) error {
	var err error

	// first we evaluate Min and Max
	if r.Min.Len() > 0 {
		r.encodedMin, err = encoder.EncodeValueBuffer(r.Min)
		if err != nil {
			return err
		}
		r.rangeTypes = r.Min.Types()
	}

	if r.Max.Len() > 0 {
		r.encodedMax, err = encoder.EncodeValueBuffer(r.Max)
		if err != nil {
			return err
		}

		if len(r.rangeTypes) > 0 {
			maxTypes := r.Max.Types()

			if len(maxTypes) != len(r.rangeTypes) {
				panic("range types for max and min differ in size")
			}

			for i, typ := range maxTypes {
				if typ != r.rangeTypes[i] {
					panic("range contain values of different types")
				}
			}
		}

		r.rangeTypes = r.Max.Types()
	}

	// ensure boundaries are typed
	// TODO(JH)
	// if r.Min.Type.IsZero() {
	// 	r.Min.Type = r.rangeType
	// }
	// if r.Max.Type.IsZero() {
	// 	r.Max.Type = r.rangeType
	// }

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	return nil
}

func (r *IndexRange) String() string {
	if r.Exact {
		return stringutil.Sprintf("%v", r.Min)
	}

	if r.Exclusive {
		return stringutil.Sprintf("[%v, %v, true]", r.Min.Types(), r.Max.Types())
	}

	return stringutil.Sprintf("[%v, %v]", r.Min.Types(), r.Max.Types())
}

func (r *IndexRange) IsEqual(other *IndexRange) bool {
	if r.Exact != other.Exact {
		return false
	}

	for i, typ := range r.rangeTypes {
		if typ != other.rangeTypes[i] {
			return false
		}
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	// TODO(JH) may or may not this
	// if r.Min.Type != other.Min.Type {
	// 	return false
	// }

	if !r.Min.IsEqual(other.Min) {
		return false
	}

	// if r.Max.Type != other.Max.Type {
	// 	return false
	// }
	if !r.Max.IsEqual(other.Max) {
		return false
	}

	return true
}

type IndexRanges []IndexRange

// Append rng to r and return the new slice.
// Duplicate ranges are ignored.
func (r IndexRanges) Append(rng IndexRange) IndexRanges {
	// ensure we don't keep duplicate ranges
	isDuplicate := false
	for _, e := range r {
		if e.IsEqual(&rng) {
			isDuplicate = true
			break
		}
	}

	if isDuplicate {
		return r
	}

	return append(r, rng)
}

type ValueBufferEncoder interface {
	EncodeValueBuffer(vb *document.ValueBuffer) ([]byte, error)
}

// Encode each range using the given value encoder.
func (r IndexRanges) EncodeBuffer(encoder ValueBufferEncoder, env *expr.Environment) error {
	for i := range r {
		err := r[i].encode(encoder, env)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r IndexRanges) String() string {
	var sb strings.Builder

	for i, rr := range r {
		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(rr.String())
	}

	return sb.String()
}

// Cost is a best effort function to determine the cost of
// a range lookup.
func (r IndexRanges) Cost() int {
	var cost int

	for _, rng := range r {
		// if we are looking for an exact value
		// increment by 1
		if rng.Exact {
			cost++
			continue
		}

		// if there are two boundaries, increment by 50
		if rng.Min.Len() > 0 && rng.Max.Len() > 0 {
			cost += 50
			continue
		}

		// if there is only one boundary, increment by 100
		if rng.Min.Len() > 0 || rng.Max.Len() > 0 {
			cost += 100
			continue
		}

		// if there are no boundaries, increment by 200
		cost += 200
	}

	return cost
}

func (r *IndexRange) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if r.encodedMin != nil {
		cmpMin = bytes.Compare(value, r.encodedMin)
	}

	// if exact is true the value has to be equal to the lower bound.
	if r.Exact {
		return cmpMin == 0
	}

	// if exclusive and the value is equal to the lower bound
	// we can ignore it
	if r.Exclusive && cmpMin == 0 {
		return false
	}

	// the value is bigger than the lower bound,
	// see if it matches the upper bound.
	if r.encodedMax != nil {
		if r.IndexArityMax < r.Arity {
			cmpMax = bytes.Compare(value[:len(r.encodedMax)-1], r.encodedMax)
		} else {
			cmpMax = bytes.Compare(value, r.encodedMax)
		}
	}

	// if boundaries are strict, ignore values equal to the max
	if r.Exclusive && cmpMax == 0 {
		return false
	}

	return cmpMin >= 0 && cmpMax <= 0
}
