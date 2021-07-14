package document

import (
	"bytes"
	"strings"
)

type operator uint8

const (
	operatorEq operator = iota + 1
	operatorGt
	operatorGte
	operatorLt
	operatorLte
)

func (op operator) String() string {
	switch op {
	case operatorEq:
		return "="
	case operatorGt:
		return ">"
	case operatorGte:
		return ">="
	case operatorLt:
		return "<"
	case operatorLte:
		return "<="
	}

	return ""
}

// IsEqual returns true if v is equal to the given value.
func IsEqual(v, other Value) (bool, error) {
	return compare(operatorEq, v, other)
}

// IsNotEqual returns true if v is not equal to the given value.
func IsNotEqual(v, other Value) (bool, error) {
	ok, err := IsEqual(v, other)
	if err != nil {
		return ok, err
	}

	return !ok, nil
}

// IsGreaterThan returns true if v is greather than the given value.
func IsGreaterThan(v, other Value) (bool, error) {
	return compare(operatorGt, v, other)
}

// IsGreaterThanOrEqual returns true if v is greather than or equal to the given value.
func IsGreaterThanOrEqual(v, other Value) (bool, error) {
	return compare(operatorGte, v, other)
}

// IsLesserThan returns true if v is lesser than the given value.
func IsLesserThan(v, other Value) (bool, error) {
	return compare(operatorLt, v, other)
}

// IsLesserThanOrEqual returns true if v is lesser than or equal to the given value.
func IsLesserThanOrEqual(v, other Value) (bool, error) {
	return compare(operatorLte, v, other)
}

func compare(op operator, l, r Value) (bool, error) {
	switch {
	// deal with nil
	case l.Type() == NullValue || r.Type() == NullValue:
		return compareWithNull(op, l, r), nil

	// compare booleans together
	case l.Type() == BoolValue && r.Type() == BoolValue:
		return compareBooleans(op, l.V().(bool), r.V().(bool)), nil

	// compare texts together
	case l.Type() == TextValue && r.Type() == TextValue:
		return compareTexts(op, l.V().(string), r.V().(string)), nil

	// compare blobs together
	case r.Type() == BlobValue && l.Type() == BlobValue:
		return compareBlobs(op, l.V().([]byte), r.V().([]byte)), nil

	// compare integers together
	case l.Type() == IntegerValue && r.Type() == IntegerValue:
		return compareIntegers(op, l.V().(int64), r.V().(int64)), nil

	// compare numbers together
	case l.Type().IsNumber() && r.Type().IsNumber():
		return compareNumbers(op, l, r), nil

	// compare arrays together
	case l.Type() == ArrayValue && r.Type() == ArrayValue:
		return compareArrays(op, l.V().(Array), r.V().(Array))

	// compare documents together
	case l.Type() == DocumentValue && r.Type() == DocumentValue:
		return compareDocuments(op, l.V().(Document), r.V().(Document))
	}

	return false, nil
}

func compareWithNull(op operator, l, r Value) bool {
	switch op {
	case operatorEq, operatorGte, operatorLte:
		return l.Type() == r.Type()
	case operatorGt, operatorLt:
		return false
	}

	return false
}

func compareBooleans(op operator, a, b bool) bool {
	switch op {
	case operatorEq:
		return a == b
	case operatorGt:
		return a && !b
	case operatorGte:
		return a == b || a
	case operatorLt:
		return !a && b
	case operatorLte:
		return a == b || !a
	}

	return false
}

func compareTexts(op operator, l, r string) bool {
	switch op {
	case operatorEq:
		return l == r
	case operatorGt:
		return strings.Compare(l, r) > 0
	case operatorGte:
		return strings.Compare(l, r) >= 0
	case operatorLt:
		return strings.Compare(l, r) < 0
	case operatorLte:
		return strings.Compare(l, r) <= 0
	}

	return false
}

func compareBlobs(op operator, l, r []byte) bool {
	switch op {
	case operatorEq:
		return bytes.Equal(l, r)
	case operatorGt:
		return bytes.Compare(l, r) > 0
	case operatorGte:
		return bytes.Compare(l, r) >= 0
	case operatorLt:
		return bytes.Compare(l, r) < 0
	case operatorLte:
		return bytes.Compare(l, r) <= 0
	}

	return false
}

func compareIntegers(op operator, l, r int64) bool {
	switch op {
	case operatorEq:
		return l == r
	case operatorGt:
		return l > r
	case operatorGte:
		return l >= r
	case operatorLt:
		return l < r
	case operatorLte:
		return l <= r
	}

	return false
}

func compareNumbers(op operator, l, r Value) bool {
	l, _ = CastAsDouble(l)
	r, _ = CastAsDouble(r)

	af := l.V().(float64)
	bf := r.V().(float64)

	var ok bool

	switch op {
	case operatorEq:
		ok = af == bf
	case operatorGt:
		ok = af > bf
	case operatorGte:
		ok = af >= bf
	case operatorLt:
		ok = af < bf
	case operatorLte:
		ok = af <= bf
	}

	return ok
}

func compareArrays(op operator, l Array, r Array) (bool, error) {
	var i, j int

	for {
		lv, lerr := l.GetByIndex(i)
		rv, rerr := r.GetByIndex(j)
		if lerr == nil {
			i++
		}
		if rerr == nil {
			j++
		}
		if lerr != nil || rerr != nil {
			break
		}
		if lv.Type() == rv.Type() || (lv.Type().IsNumber() && rv.Type().IsNumber()) {
			isEq, err := compare(operatorEq, lv, rv)
			if err != nil {
				return false, err
			}
			if !isEq && op != operatorEq {
				return compare(op, lv, rv)
			}
			if !isEq {
				return false, nil
			}
		} else {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt, operatorGte:
				return lv.Type() > rv.Type(), nil
			case operatorLt, operatorLte:
				return lv.Type() < rv.Type(), nil
			}
		}
	}

	switch {
	case i > j:
		switch op {
		case operatorEq, operatorLt, operatorLte:
			return false, nil
		default:
			return true, nil
		}
	case i < j:
		switch op {
		case operatorEq, operatorGt, operatorGte:
			return false, nil
		default:
			return true, nil
		}
	default:
		switch op {
		case operatorEq, operatorGte, operatorLte:
			return true, nil
		default:
			return false, nil
		}
	}
}

func compareDocuments(op operator, l, r Document) (bool, error) {
	lf, err := Fields(l)
	if err != nil {
		return false, err
	}
	rf, err := Fields(r)
	if err != nil {
		return false, err
	}

	if len(lf) == 0 && len(rf) > 0 {
		switch op {
		case operatorEq:
			return false, nil
		case operatorGt:
			return false, nil
		case operatorGte:
			return false, nil
		case operatorLt:
			return true, nil
		case operatorLte:
			return true, nil
		}
	}

	if len(rf) == 0 && len(lf) > 0 {
		switch op {
		case operatorEq:
			return false, nil
		case operatorGt:
			return true, nil
		case operatorGte:
			return true, nil
		case operatorLt:
			return false, nil
		case operatorLte:
			return false, nil
		}
	}

	var i, j int

	for i < len(lf) && j < len(rf) {
		if cmp := strings.Compare(lf[i], rf[j]); cmp != 0 {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt:
				return cmp > 0, nil
			case operatorGte:
				return cmp >= 0, nil
			case operatorLt:
				return cmp < 0, nil
			case operatorLte:
				return cmp <= 0, nil
			}
		}

		lv, lerr := l.GetByField(lf[i])
		rv, rerr := r.GetByField(rf[j])
		if lerr == nil {
			i++
		}
		if rerr == nil {
			j++
		}
		if lerr != nil || rerr != nil {
			break
		}
		if lv.Type() == rv.Type() || (lv.Type().IsNumber() && rv.Type().IsNumber()) {
			isEq, err := compare(operatorEq, lv, rv)
			if err != nil {
				return false, err
			}
			if !isEq && op != operatorEq {
				return compare(op, lv, rv)
			}
			if !isEq {
				return false, nil
			}
		} else {
			switch op {
			case operatorEq:
				return false, nil
			case operatorGt, operatorGte:
				return lv.Type() > rv.Type(), nil
			case operatorLt, operatorLte:
				return lv.Type() < rv.Type(), nil
			}
		}
	}

	switch {
	case i > j:
		switch op {
		case operatorEq, operatorLt, operatorLte:
			return false, nil
		default:
			return true, nil
		}
	case i < j:
		switch op {
		case operatorEq, operatorGt, operatorGte:
			return false, nil
		default:
			return true, nil
		}
	default:
		switch op {
		case operatorEq, operatorGte, operatorLte:
			return true, nil
		default:
			return false, nil
		}
	}
}
