package stream

import (
	"bytes"
	"math"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/stringutil"
)

type Costable interface {
	Cost() int
}

type ValueRange struct {
	Min, Max expr.Expr
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool
}

func (r *ValueRange) evalRange(table *database.Table, env *environment.Environment) (*encodedValueRange, bool, error) {
	var err error

	pk := table.Info.FieldConstraints.GetPrimaryKey()

	rng := encodedValueRange{
		pkType:      pk.Type,
		path:        pk.Path,
		constraints: table.Info.FieldConstraints,

		Exclusive: r.Exclusive,
		Exact:     r.Exact,
	}

	if r.Min != nil {
		rng.Min, err = r.Min.Eval(env)
		if err != nil {
			return nil, false, err
		}

		var ok bool
		rng.Min, ok, err = rng.Convert(rng.Min, true)
		if err != nil || !ok {
			return nil, ok, err
		}
	}

	if r.Max != nil {
		rng.Max, err = r.Max.Eval(env)
		if err != nil {
			return nil, false, err
		}

		var ok bool
		rng.Max, ok, err = rng.Convert(rng.Max, false)
		if err != nil || !ok {
			return nil, ok, err
		}
	}

	return &rng, true, nil
}

func (r *ValueRange) encode(table *database.Table, env *environment.Environment) (*encodedValueRange, error) {
	rng, ok, err := r.evalRange(table, env)
	if err != nil || !ok {
		return nil, err
	}

	if rng.Min != nil {
		rng.EncodedMin, err = table.EncodeValue(rng.Min)
		if err != nil {
			return nil, err
		}
		rng.RangeType = rng.Min.Type()
	}
	if rng.Max != nil {
		rng.EncodedMax, err = table.EncodeValue(rng.Max)
		if err != nil {
			return nil, err
		}
		if !rng.RangeType.IsAny() && rng.RangeType != rng.Max.Type() {
			panic("range contain values of different types")
		}

		rng.RangeType = rng.Max.Type()
	}

	// ensure boundaries are typed
	if rng.Min == nil {
		rng.Min = document.NewEmptyValue(rng.RangeType)
	}
	if rng.Max == nil {
		rng.Max = document.NewEmptyValue(rng.RangeType)
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	return rng, nil
}

func (r *ValueRange) String() string {
	if r.Exact {
		return stringutil.Sprintf("%v", r.Min)
	}

	var min, max string = "-1", "-1"

	if r.Min != nil {
		min = r.Min.String()
	}
	if r.Max != nil {
		max = r.Max.String()
	}

	if r.Exclusive {
		return stringutil.Sprintf("[%v, %v, true]", min, max)
	}

	return stringutil.Sprintf("[%v, %v]", min, max)
}

func (r *ValueRange) IsEqual(other *ValueRange) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if expr.Equal(r.Min, other.Min) {
		return false
	}

	if expr.Equal(r.Max, other.Max) {
		return false
	}

	return true
}

func (r ValueRange) Clone() ValueRange {
	return r
}

type encodedValueRange struct {
	pkType      document.ValueType
	path        document.Path
	constraints database.FieldConstraints

	Min, Max  document.Value
	Exclusive bool
	Exact     bool

	EncodedMin, EncodedMax []byte
	RangeType              document.ValueType
}

func (r *encodedValueRange) Convert(v document.Value, isMin bool) (document.Value, bool, error) {
	// ensure the operand satisfies all the constraints, index can work only on exact types.
	// if a number is encountered, try to convert it to the right type if and only if the conversion
	// is lossless.
	v, err := r.constraints.ConvertValueAtPath(r.path, v, func(v document.Value, path document.Path, targetType document.ValueType) (document.Value, error) {
		if v.Type() == document.IntegerValue && targetType == document.DoubleValue {
			return document.CastAsDouble(v)
		}

		if v.Type() == document.DoubleValue && targetType == document.IntegerValue {
			f := v.V().(float64)
			if float64(int64(f)) == f {
				return document.CastAsInteger(v)
			}

			if r.Exact {
				return v, nil
			}

			// we want to convert a non rounded double to int in a way that preserves
			// comparison logic with the index. ex:
			// a > 1.1  -> a >= 2; exclusive -> false
			// a >= 1.1 -> a >= 2; exclusive -> false
			// a < 1.1  -> a < 2;  exclusive -> true
			// a <= 1.1 -> a < 2;  exclusive -> true
			// a BETWEEN 1.1 AND 2.2 -> a >= 2 AND a <= 3; exclusive -> false

			// First, we need to ceil the number. Ex: 1.1 -> 2
			v = document.NewIntegerValue(int64(math.Ceil(f)))

			// Next, we need to convert the boundaries
			if isMin {
				// (a > 1.1) or (a >= 1.1) must be transformed to (a >= 2)
				r.Exclusive = false
			} else {
				// (a < 1.1) or (a <= 1.1) must be transformed to (a < 2)
				// But there is an exception: if we are dealing with both min
				// and max boundaries, we are operating a BETWEEN operation,
				// meaning that we need to convert a BETWEEN 1.1 AND 2.2 to a >= 2 AND a <= 3,
				// and thus have to set exclusive to false.
				r.Exclusive = r.Min == nil
			}
		}

		return v, nil
	})
	if err != nil {
		return v, false, err
	}

	// if the index is not typed, any operand can work
	if r.pkType.IsAny() {
		return v, true, nil
	}

	// if the index is typed, it must be of the same type as the converted value
	return v, r.pkType == v.Type(), nil
}

func (r *encodedValueRange) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if r.EncodedMin != nil {
		cmpMin = bytes.Compare(value, r.EncodedMin)
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
	if r.EncodedMax != nil {
		cmpMax = bytes.Compare(value, r.EncodedMax)
	}

	// if boundaries are strict, ignore values equal to the max
	if r.Exclusive && cmpMax == 0 {
		return false
	}

	return cmpMax <= 0
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

// Encode each range using the given value encoder.
func (r ValueRanges) Encode(table *database.Table, env *environment.Environment) ([]*encodedValueRange, error) {
	ranges := make([]*encodedValueRange, 0, len(r))

	for i := range r {
		rng, err := r[i].encode(table, env)
		if err != nil {
			return nil, err
		}
		if rng != nil {
			ranges = append(ranges, rng)
		}
	}

	return ranges, nil
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
		if rng.Min != nil && rng.Max != nil {
			cost += 50
		}

		// if there is only one boundary, increment by 100
		if (rng.Min != nil && rng.Max == nil) || (rng.Min == nil && rng.Max != nil) {
			cost += 100
			continue
		}

		// if there are no boundaries, increment by 200
		cost += 200
	}

	return cost
}

// IndexRange represents a range to select indexed values after or before
// a given boundary. Because indexes can be composites, IndexRange boundaries
// are composite as well.
type IndexRange struct {
	Min, Max expr.LiteralExprList
	Paths    []document.Path
	// Exclude Min and Max from the results.
	// By default, min and max are inclusive.
	// Exclusive and Exact cannot be set to true at the same time.
	Exclusive bool
	// Used to match an exact value equal to Min.
	// If set to true, Max will be ignored for comparison
	// and for determining the global upper bound.
	Exact bool

	// IndexArity is the underlying index arity, which can be greater
	// than the boundaries of this range.
	IndexArity int
}

func (r *IndexRange) evalRange(index *database.Index, table *database.Table, env *environment.Environment) (*encodedIndexRange, bool, error) {
	rng := encodedIndexRange{
		constraints: table.Info.FieldConstraints,

		Exclusive:  r.Exclusive,
		Exact:      r.Exact,
		IndexArity: r.IndexArity,
	}

	if r.Min != nil {
		lv, err := r.Min.Eval(env)
		if err != nil {
			return nil, false, err
		}
		rng.Min = lv.V().(*document.ValueBuffer)

		var ok bool
		for i := range rng.Min.Values {
			rng.Min.Values[i], ok, err = rng.Convert(rng.Min.Values[i], index.Info.Paths[i], index.Info.Types[i], true)
			if err != nil || !ok {
				return nil, ok, err
			}
		}
	}

	if r.Max != nil {
		lv, err := r.Max.Eval(env)
		if err != nil {
			return nil, false, err
		}
		rng.Max = lv.V().(*document.ValueBuffer)

		var ok bool
		for i := range rng.Max.Values {
			rng.Max.Values[i], ok, err = rng.Convert(rng.Max.Values[i], index.Info.Paths[i], index.Info.Types[i], false)
			if err != nil || !ok {
				return nil, ok, err
			}
		}
	}

	return &rng, true, nil
}

func (r *IndexRange) encode(index *database.Index, table *database.Table, env *environment.Environment) (*encodedIndexRange, error) {
	rng, ok, err := r.evalRange(index, table, env)
	if err != nil || !ok {
		return nil, err
	}

	if len(r.Min) > 0 {
		rng.EncodedMin, err = index.EncodeValueBuffer(rng.Min)
		if err != nil {
			return nil, err
		}
		rng.RangeTypes = rng.Min.Types()
	}

	if len(r.Max) > 0 {
		rng.EncodedMax, err = index.EncodeValueBuffer(rng.Max)
		if err != nil {
			return nil, err
		}

		if len(rng.RangeTypes) > 0 {
			maxTypes := rng.Max.Types()

			if len(maxTypes) != len(rng.RangeTypes) {
				panic("range types for max and min differ in size")
			}

			for i, typ := range maxTypes {
				if typ != rng.RangeTypes[i] {
					panic("range contain values of different types")
				}
			}
		}

		rng.RangeTypes = rng.Max.Types()
	}

	// Ensure boundaries are typed, at least with the first type
	if len(r.Max) == 0 && len(r.Min) > 0 {
		v, err := rng.Min.GetByIndex(0)
		if err != nil {
			return nil, err
		}

		rng.Max = document.NewValueBuffer(document.NewEmptyValue(v.Type()))
	}

	if len(r.Min) == 0 && len(r.Max) > 0 {
		v, err := rng.Max.GetByIndex(0)
		if err != nil {
			return nil, err
		}

		rng.Min = document.NewValueBuffer(document.NewEmptyValue(v.Type()))
	}

	if r.Exclusive && r.Exact {
		panic("exclusive and exact cannot both be true")
	}

	return rng, nil
}

func (r *IndexRange) String() string {
	format := func(el expr.LiteralExprList) string {
		switch len(el) {
		case 0:
			return "-1"
		case 1:
			return el[0].String()
		default:
			return el.String()
		}
	}

	if r.Exact {
		return stringutil.Sprintf("%v", format(r.Min))
	}

	if r.Exclusive {
		return stringutil.Sprintf("[%v, %v, true]", format(r.Min), format(r.Max))
	}

	return stringutil.Sprintf("[%v, %v]", format(r.Min), format(r.Max))
}

func (r *IndexRange) IsEqual(other *IndexRange) bool {
	if r.Exact != other.Exact {
		return false
	}

	if r.Exclusive != other.Exclusive {
		return false
	}

	if len(r.Min) != len(other.Min) {
		return false
	}

	if len(r.Max) != len(other.Max) {
		return false
	}

	if !r.Min.IsEqual(other.Min) {
		return false
	}

	if !r.Max.IsEqual(other.Max) {
		return false
	}

	return true
}

func (r IndexRange) Clone() IndexRange {
	return r
}

type encodedIndexRange struct {
	constraints database.FieldConstraints

	Min, Max *document.ValueBuffer

	Exclusive  bool
	Exact      bool
	IndexArity int

	EncodedMin, EncodedMax []byte
	RangeTypes             []document.ValueType
}

func (r *encodedIndexRange) Convert(v document.Value, p document.Path, t document.ValueType, isMin bool) (document.Value, bool, error) {
	// ensure the operand satisfies all the constraints, index can work only on exact types.
	// if a number is encountered, try to convert it to the right type if and only if the conversion
	// is lossless.
	v, err := r.constraints.ConvertValueAtPath(p, v, func(v document.Value, path document.Path, targetType document.ValueType) (document.Value, error) {
		if v.Type() == document.IntegerValue && targetType == document.DoubleValue {
			return document.CastAsDouble(v)
		}

		if v.Type() == document.DoubleValue && targetType == document.IntegerValue {
			f := v.V().(float64)
			if float64(int64(f)) == f {
				return document.CastAsInteger(v)
			}

			if r.Exact {
				return v, nil
			}

			// we want to convert a non rounded double to int in a way that preserves
			// comparison logic with the index. ex:
			// a > 1.1  -> a >= 2; exclusive -> false
			// a >= 1.1 -> a >= 2; exclusive -> false
			// a < 1.1  -> a < 2;  exclusive -> true
			// a <= 1.1 -> a < 2;  exclusive -> true
			// a BETWEEN 1.1 AND 2.2 -> a >= 2 AND a <= 3; exclusive -> false

			// First, we need to ceil the number. Ex: 1.1 -> 2
			v = document.NewIntegerValue(int64(math.Ceil(f)))

			// Next, we need to convert the boundaries
			if isMin {
				// (a > 1.1) or (a >= 1.1) must be transformed to (a >= 2)
				r.Exclusive = false
			} else {
				// (a < 1.1) or (a <= 1.1) must be transformed to (a < 2)
				// But there is an exception: if we are dealing with both min
				// and max boundaries, we are operating a BETWEEN operation,
				// meaning that we need to convert a BETWEEN 1.1 AND 2.2 to a >= 2 AND a <= 3,
				// and thus have to set exclusive to false.
				r.Exclusive = r.Min == nil || r.Min.Len() == 0
			}
		}

		return v, nil
	})
	if err != nil {
		return v, false, err
	}

	// if the index is not typed, any operand can work
	if t.IsAny() {
		return v, true, nil
	}

	// if the index is typed, it must be of the same type as the converted value
	return v, t == v.Type(), nil
}

func (r *encodedIndexRange) IsInRange(value []byte) bool {
	// by default, we consider the value within range
	cmpMin, cmpMax := 1, -1

	// we compare with the lower bound and see if it matches
	if r.EncodedMin != nil {
		cmpMin = bytes.Compare(value, r.EncodedMin)
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
	if r.EncodedMax != nil {
		if r.Max.Len() < r.IndexArity {
			cmpMax = bytes.Compare(value[:len(r.EncodedMax)], r.EncodedMax)
		} else {
			cmpMax = bytes.Compare(value, r.EncodedMax)
		}
	}

	// if boundaries are strict, ignore values equal to the max
	if r.Exclusive && cmpMax == 0 {
		return false
	}

	return cmpMin >= 0 && cmpMax <= 0
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
func (r IndexRanges) EncodeBuffer(index *database.Index, table *database.Table, env *environment.Environment) ([]*encodedIndexRange, error) {
	ranges := make([]*encodedIndexRange, 0, len(r))

	for i := range r {
		enc, err := r[i].encode(index, table, env)
		if err != nil {
			return nil, err
		}
		if enc != nil {
			ranges = append(ranges, enc)
		}
	}

	return ranges, nil
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
		if len(rng.Min) > 0 && len(rng.Max) > 0 {
			cost += 50
			continue
		}

		// if there is only one boundary, increment by 100
		if len(rng.Min) > 0 || len(rng.Max) > 0 {
			cost += 100
			continue
		}

		// if there are no boundaries, increment by 200
		cost += 200
	}

	return cost
}
