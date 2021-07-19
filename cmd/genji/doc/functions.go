package doc

type functionDocs map[string]string

var packageDocs = map[string]functionDocs{
	"math": mathDocs,
	"":     builtinDocs,
}

var builtinDocs = functionDocs{
	"pk":    "The pk() function returns the primary key for the current document",
	"count": "Returns a count of the number of times that arg1 is not NULL in a group. The count(*) function (with no arguments) returns the total number of rows in the group.",
	"min":   "Returns the minimum value of the arg1 expression in a group.",
	"max":   "Returns the maximum value of the arg1 expressein in a group.",
	"sum":   "The sum function returns the sum of all values taken by the arg1 expression in a group.",
	"avg":   "The avg function returns the average of all values taken by the arg1 expression in a group.",
}

var mathDocs = functionDocs{
	"abs":   "Returns the absolute value of arg1.",
	"acos":  "Returns the arcosine, in radiant, of arg1.",
	"acosh": "Returns the inverse hyperbolic cosine of arg1.",
	"asin":  "Returns the arsine, in radiant, of arg1.",
	"asinh": "Returns the inverse hyperbolic sine of arg1.",
	"atan":  "Returns the arctangent, in radians, of arg1.",
	"atan2": "Returns the arctangent of arg1/arg2, using the signs of the two to determine the quadrant of the return value.",
	"floor": "Returns the greatest integer value less than or equal to arg1.",
}
