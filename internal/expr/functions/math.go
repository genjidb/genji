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
	"acosh": acosh,
	"asin":  asin,
	"asinh": asinh,
	"atan":  atan,
	"atan2": atan2,
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

var acosh = &ScalarDefinition{
	name:  "acosh",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			if v < 1 {
				return document.Value{}, stringutil.Errorf("out of range, acosh(arg1) expects arg1 >= 1")
			}
			return document.NewDoubleValue(math.Acosh(float64(v))), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			if v < 1.0 {
				return document.Value{}, stringutil.Errorf("out of range, acosh(arg1) expects arg1 >= 1")
			}
			return document.NewDoubleValue(math.Acosh(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("acosh(arg1) expects arg1 to be a number >= 1")
		}
	},
}

var asin = &ScalarDefinition{
	name:  "asin",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			if v > 1 || v < -1 {
				return document.Value{}, stringutil.Errorf("out of range, asin(arg1) expects arg1 to be within [-1, 1]")
			}
			return document.NewDoubleValue(math.Asin(float64(v))), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			if v > 1.0 || v < -1.0 {
				return document.Value{}, stringutil.Errorf("out of range, asin(arg1) expects arg1 to be within [-1, 1]")
			}
			return document.NewDoubleValue(math.Asin(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("asin(arg1) expects arg1 to be a number within [-1, 1]")
		}
	},
}

var asinh = &ScalarDefinition{
	name:  "asinh",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			return document.NewDoubleValue(math.Asinh(float64(v))), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			return document.NewDoubleValue(math.Asinh(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("asinh(arg1) expects arg1 to be a number")
		}
	},
}

var atan = &ScalarDefinition{
	name:  "atan",
	arity: 1,
	callFn: func(args ...document.Value) (document.Value, error) {
		switch args[0].Type {
		case document.NullValue:
			return document.NewNullValue(), nil
		case document.IntegerValue:
			v := args[0].V.(int64)
			return document.NewDoubleValue(math.Atan(float64(v))), nil
		case document.DoubleValue:
			v := args[0].V.(float64)
			return document.NewDoubleValue(math.Atan(v)), nil
		default:
			return document.Value{}, stringutil.Errorf("atan(arg1) expects arg1 to be a number")
		}
	},
}

var atan2 = &ScalarDefinition{
	name:  "atan2",
	arity: 2,
	callFn: func(args ...document.Value) (document.Value, error) {
		if args[0].Type == document.NullValue || args[1].Type == document.NullValue {
			return document.NewNullValue(), nil
		}
		if !args[0].Type.IsNumber() || !args[1].Type.IsNumber() {
			return document.Value{}, stringutil.Errorf("atan2(arg1, arg2) expects arg1 and arg2 to be numbers")
		}

		var a, b float64
		if args[0].Type == document.IntegerValue {
			a = float64(args[0].V.(int64))
		} else {
			a = args[0].V.(float64)
		}
		if args[1].Type == document.IntegerValue {
			b = float64(args[1].V.(int64))
		} else {
			b = args[1].V.(float64)
		}
		return document.NewDoubleValue(math.Atan2(a, b)), nil
	},
}
