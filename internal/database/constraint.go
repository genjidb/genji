package database

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

type ConstraintViolationError struct {
	Constraint string
	Path       document.Path
}

func (c *ConstraintViolationError) Error() string {
	return stringutil.Sprintf("%s constraint error: %s", c.Constraint, c.Path)
}

// FieldConstraint describes constraints on a particular field.
type FieldConstraint struct {
	Path         document.Path
	Type         types.ValueType
	IsPrimaryKey bool
	IsNotNull    bool
	IsUnique     bool
	DefaultValue TableExpression
	Identity     *FieldConstraintIdentity
	IsInferred   bool
	InferredBy   []document.Path
}

// IsEqual compares f with other member by member.
// Inference is not compared.
func (f *FieldConstraint) IsEqual(other *FieldConstraint) bool {
	if !f.Path.IsEqual(other.Path) {
		return false
	}

	if f.Type != other.Type {
		return false
	}

	if f.IsPrimaryKey != other.IsPrimaryKey {
		return false
	}

	if f.IsNotNull != other.IsNotNull {
		return false
	}

	if f.HasDefaultValue() != other.HasDefaultValue() {
		return false
	}

	if f.HasDefaultValue() {
		if !f.DefaultValue.IsEqual(other.DefaultValue) {
			return false
		}
	}

	if !f.Identity.IsEqual(other.Identity) {
		return false
	}

	return true
}

func (f *FieldConstraint) String() string {
	var s strings.Builder

	s.WriteString(f.Path.String())
	s.WriteString(" ")
	s.WriteString(strings.ToUpper(f.Type.String()))

	if f.IsNotNull {
		s.WriteString(" NOT NULL")
	}

	if f.IsPrimaryKey {
		s.WriteString(" PRIMARY KEY")
	}

	if f.IsUnique {
		s.WriteString(" UNIQUE")
	}

	if f.HasDefaultValue() {
		s.WriteString(" DEFAULT ")
		s.WriteString(f.DefaultValue.String())
	}

	return s.String()
}

// MergeInferred adds the other.InferredBy to f.InferredBy and ensures there are no duplicates.
func (f *FieldConstraint) MergeInferred(other *FieldConstraint) {
	for _, by := range other.InferredBy {
		duplicate := false
		for _, fby := range f.InferredBy {
			if fby.IsEqual(by) {
				duplicate = true
				break
			}
		}

		if !duplicate {
			f.InferredBy = append(f.InferredBy, by)
		}
	}
}

// HasDefaultValue returns this field contains a default value constraint.
func (f *FieldConstraint) HasDefaultValue() bool {
	return f.DefaultValue != nil
}

// FieldConstraints is a list of field constraints.
type FieldConstraints []*FieldConstraint

// NewFieldConstraints takes user-defined field constraints, validates them, infers additional
// constraints if needed, and returns a valid FieldConstraints type that can be assigned to a table.
func NewFieldConstraints(userConstraints []*FieldConstraint) (FieldConstraints, error) {
	return FieldConstraints(userConstraints).Infer()
}

// Get a field constraint by path. Returns nil if not found.
func (f FieldConstraints) Get(path document.Path) *FieldConstraint {
	for _, fc := range f {
		if fc.Path.IsEqual(path) {
			return fc
		}
	}

	return nil
}

// GetPrimaryKey returns the field constraint of the primary key.
// Returns nil if there is no primary key.
func (f FieldConstraints) GetPrimaryKey() *FieldConstraint {
	for _, fc := range f {
		if fc.IsPrimaryKey {
			return fc
		}
	}

	return nil
}

// Infer additional constraints based on user defined ones.
// For example, given the following table:
//   CREATE TABLE foo (a.b[0] TEXT)
// this function will return a TableInfo that behaves as if the table
// had been created like this:
//   CREATE TABLE foo(
//      a DOCUMENT
//      a.b ARRAY
//      a.b[0] TEXT
//   )
func (f FieldConstraints) Infer() (FieldConstraints, error) {
	newConstraints := make(FieldConstraints, 0, len(f))

	for _, fc := range f {
		// loop over all the path fragments and
		// create intermediary inferred constraints.
		if len(fc.Path) > 1 {
			for i := range fc.Path {
				// stop before reaching the last fragment
				// which will be added outside of this loop
				if i+1 == len(fc.Path) {
					break
				}

				newFc := FieldConstraint{
					Path:       fc.Path[:i+1],
					IsInferred: true,
					InferredBy: []document.Path{fc.Path},
				}
				if fc.Path[i+1].FieldName != "" {
					newFc.Type = types.DocumentValue
				} else {
					newFc.Type = types.ArrayValue
				}

				err := newConstraints.Add(&newFc)
				if err != nil {
					return nil, err
				}
			}
		}

		// add the non inferred path to the list
		// and ensure there are no conflicts with
		// existing ones.
		err := newConstraints.Add(fc)
		if err != nil {
			return nil, err
		}
	}

	return newConstraints, nil
}

// Add a field constraint to the list. If another constraint exists for the same path
// and they are equal, newFc will be ignored. Otherwise an error will be returned.
// If newFc has been inferred by another constraint and another constraint exists with the same
// path, their InferredBy member will be merged.
func (f *FieldConstraints) Add(newFc *FieldConstraint) error {
	for i, c := range *f {
		if c.Path.IsEqual(newFc.Path) {
			// if both non inferred, they are duplicate
			if !newFc.IsInferred && !c.IsInferred {
				return stringutil.Errorf("conflicting constraints: %q and %q", c.String(), newFc.String())
			}

			// determine which one is inferred
			inferredFc, nonInferredFc := c, newFc
			if newFc.IsInferred {
				inferredFc, nonInferredFc = nonInferredFc, inferredFc
			}

			// the inferred one may have less constraints that the user-defined one
			inferredFc.DefaultValue = nonInferredFc.DefaultValue
			inferredFc.IsNotNull = nonInferredFc.IsNotNull
			inferredFc.IsPrimaryKey = nonInferredFc.IsPrimaryKey

			// detect if constraints are different
			if !c.IsEqual(newFc) {
				return stringutil.Errorf("conflicting constraints: %q and %q", c.String(), newFc.String())
			}

			// if both inferred, merge the InferredBy member
			if newFc.IsInferred && c.IsInferred {
				c.MergeInferred(newFc)
				return nil
			}

			// if existing one is not inferred, ignore newFc
			if newFc.IsInferred && !c.IsInferred {
				return nil
			}

			// if existing one is inferred, and newFc is not,
			// replace it
			(*f)[i] = newFc
			return nil
		}

		// ensure we don't have duplicate primary keys
		if c.IsPrimaryKey && newFc.IsPrimaryKey {
			return stringutil.Errorf(
				"multiple primary keys are not allowed (%q is primary key)",
				c.Path.String(),
			)
		}
	}

	// ensure default value type is compatible
	if newFc.DefaultValue != nil && !newFc.Type.IsAny() {
		// first, try to evaluate the default value
		v, err := newFc.DefaultValue.Eval(nil)
		// if there is no error, check if the default value can be converted to the type of the constraint
		if err == nil {
			_, err = document.CastAs(v, newFc.Type)
			if err != nil {
				return stringutil.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		} else {
			// if there is an error, we know we are using a function that returns an integer (NEXT VALUE FOR)
			// which is the only one compatible for the moment.
			// Integers can be converted to other integers, doubles, texts and bools.
			switch newFc.Type {
			case types.IntegerValue, types.DoubleValue, types.TextValue, types.BoolValue:
			default:
				return stringutil.Errorf("default value %q cannot be converted to type %q", newFc.DefaultValue, newFc.Type)
			}
		}
	}

	*f = append(*f, newFc)
	return nil
}

// ValidateDocument calls Convert then ensures the document validates against the field constraints.
func (f FieldConstraints) ValidateDocument(tx *Transaction, d document.Document) (*document.FieldBuffer, error) {
	fb := document.NewFieldBuffer()
	err := fb.Copy(d)
	if err != nil {
		return nil, err
	}

	// generate default values for all fields
	for _, fc := range f {
		if fc.DefaultValue == nil {
			continue
		}

		_, err := fc.Path.GetValueFromDocument(fb)
		if err == nil {
			continue
		}

		if err != document.ErrFieldNotFound {
			return nil, err
		}

		v, err := fc.DefaultValue.Eval(tx)
		if err != nil {
			return nil, err
		}
		err = fb.Set(fc.Path, v)
		if err != nil {
			return nil, err
		}
	}

	fb, err = f.ConvertDocument(fb)
	if err != nil {
		return nil, err
	}

	// ensure no field is missing
	for _, fc := range f {
		if !fc.IsNotNull {
			continue
		}

		v, err := fc.Path.GetValueFromDocument(fb)
		if err == nil {
			// if field is found, it has already been converted
			// to the right type above.
			// check if it is required but null.
			if v.Type() == types.NullValue {
				return nil, &ConstraintViolationError{"NOT NULL", fc.Path}
			}

			continue
		}

		if err != document.ErrFieldNotFound {
			return nil, err
		}

		return nil, &ConstraintViolationError{"NOT NULL", fc.Path}
	}

	return fb, nil
}

// ConvertDocument the document using the field constraints.
// It converts any path that has a field constraint on it into the specified type using CAST.
// If there is no constraint on an integer field or value, it converts it into a double.
// Default values on missing fields are not applied.
func (f FieldConstraints) ConvertDocument(d document.Document) (*document.FieldBuffer, error) {
	return f.convertDocumentAtPath(nil, d, CastConversion)
}

// ConversionFunc is called when the type of a value is different than the expected type
// and the value needs to be converted.
type ConversionFunc func(v document.Value, path document.Path, targetType types.ValueType) (document.Value, error)

// CastConversion is a ConversionFunc that casts the value to the target type.
func CastConversion(v document.Value, path document.Path, targetType types.ValueType) (document.Value, error) {
	newV, err := document.CastAs(v, targetType)
	if err != nil {
		return v, stringutil.Errorf("field %q must be of type %q, got %q", path, targetType, v.Type())
	}

	return newV, nil
}

// ConvertValueAtPath converts the value using the field constraints that are applicable
// at the given path.
func (f FieldConstraints) ConvertValueAtPath(path document.Path, v document.Value, conversionFn ConversionFunc) (document.Value, error) {
	switch v.Type() {
	case types.ArrayValue:
		vb, err := f.convertArrayAtPath(path, v.V().(document.Array), conversionFn)
		return document.NewArrayValue(vb), err
	case types.DocumentValue:
		fb, err := f.convertDocumentAtPath(path, v.V().(document.Document), conversionFn)
		return document.NewDocumentValue(fb), err
	}
	return f.convertScalarAtPath(path, v, conversionFn)
}

// convert the value using field constraints type information.
// if there is a type constraint on a path, apply it.
// if a value is an integer and has no constraint, convert it to double.
func (f FieldConstraints) convertScalarAtPath(path document.Path, v document.Value, conversionFn ConversionFunc) (document.Value, error) {
	for _, fc := range f {
		if !fc.Path.IsEqual(path) {
			continue
		}

		// check if the constraint enforce a particular type
		// and if so convert the value to the new type.
		if fc.Type != 0 {
			newV, err := conversionFn(v, fc.Path, fc.Type)
			if err != nil {
				return v, err
			}

			return newV, nil
		}
		break
	}

	// no constraint have been found for this path.
	// check if this is an integer and convert it to double.
	if v.Type() == types.IntegerValue {
		newV, _ := document.CastAsDouble(v)
		return newV, nil
	}

	return v, nil
}

func (f FieldConstraints) convertDocumentAtPath(path document.Path, d document.Document, conversionFn ConversionFunc) (*document.FieldBuffer, error) {
	fb, ok := d.(*document.FieldBuffer)
	if !ok {
		fb = document.NewFieldBuffer()
		err := fb.Copy(d)
		if err != nil {
			return nil, err
		}
	}

	err := fb.Apply(func(p document.Path, v document.Value) (document.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return fb, err
}

func (f FieldConstraints) convertArrayAtPath(path document.Path, a document.Array, conversionFn ConversionFunc) (*document.ValueBuffer, error) {
	vb := document.NewValueBuffer()
	err := vb.Copy(a)
	if err != nil {
		return nil, err
	}

	err = vb.Apply(func(p document.Path, v document.Value) (document.Value, error) {
		return f.convertScalarAtPath(append(path, p...), v, conversionFn)
	})

	return vb, err
}

type FieldConstraintIdentity struct {
	SequenceName string
	Always       bool
}

func (f *FieldConstraintIdentity) IsEqual(other *FieldConstraintIdentity) bool {
	if f == nil {
		return other == nil
	}

	if other == nil {
		return false
	}

	return f.SequenceName == other.SequenceName && f.Always == other.Always
}

type TableExpression interface {
	Bind(catalog Catalog)
	Eval(tx *Transaction) (document.Value, error)
	IsEqual(other TableExpression) bool
	String() string
}
