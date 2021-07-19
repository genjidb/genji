package functions

import (
	"math"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
)

// MathFunctions returns all math package functions.
func MathFunctions() Definitions {
	return mathFunctions
}

var mathFunctions = Definitions{
	"floor": floor,
	"abs":   abs,
	"acos":  acos,
}

var floor = &ScalarDefinition{
	name:  "floor",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.DoubleValue:
			return document.NewDoubleValue(math.Floor(args[0].V.(float64))), nil
		case document.IntegerValue:
			return args[0], nil
		default:
			return document.Value{}, stringutil.Errorf("floor(arg1) expects arg1 to be a number")
		}
	},
}

var abs = &ScalarDefinition{
	name:  "abs",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			if v == math.MinInt64 {
				// If X is the  integer -9223372036854775808 then abs(X) throws an integer overflow
				// error since there is no equivalent positive 64-bit two complement value.
				return document.Value{}, stringutil.Errorf("integer overflow")
			}
			f := math.Abs(float64(v))
			return document.NewIntegerValue(int64(f)), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			return document.NewDoubleValue(math.Abs(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("abs(arg1) expects arg1 to be a number or NULL")
		}
	},
}

var acos = &ScalarDefinition{
	name:  "acos",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			if v > 1 || v < -1 {
				return document.Value{}, stringutil.Errorf("out of range, acos(arg1) expects arg1 to be within [-1, 1]")
			}
			return document.NewDoubleValue(math.Acos(float64(v))), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			if v > 1.0 || v < -1.0 {
				return document.Value{}, stringutil.Errorf("out of range, acos(arg1) expects arg1 to be within [-1, 1]")
			}
			return document.NewDoubleValue(math.Acos(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("acos(arg1) expects arg1 to be a number within [-1, 1]")
		}
	},
}
