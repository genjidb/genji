package database

import (
	"math"
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/stringutil"
	"github.com/genjidb/genji/types"
)

// TableInfo contains information about a table.
type TableInfo struct {
	// name of the table.
	TableName string
	// name of the store associated with the table.
	StoreName []byte
	ReadOnly  bool

	FieldConstraints FieldConstraints

	// Name of the docid sequence if any.
	DocidSequenceName string
}

func (ti *TableInfo) Type() string {
	return "table"
}

func (ti *TableInfo) Name() string {
	return ti.TableName
}

func (ti *TableInfo) SetName(name string) {
	ti.TableName = name
}

func (ti *TableInfo) GenerateBaseName() string {
	return ti.TableName
}

// String returns a SQL representation.
func (ti *TableInfo) String() string {
	var s strings.Builder

	stringutil.Fprintf(&s, "CREATE TABLE %s", stringutil.NormalizeIdentifier(ti.TableName, '`'))
	if len(ti.FieldConstraints) > 0 {
		s.WriteString(" (")
	}

	for i, fc := range ti.FieldConstraints {
		if fc.IsInferred {
			continue
		}

		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(fc.String())
	}

	if len(ti.FieldConstraints) > 0 {
		s.WriteString(")")
	}

	return s.String()
}

// Clone creates another tableInfo with the same values.
func (ti *TableInfo) Clone() *TableInfo {
	cp := *ti
	cp.FieldConstraints = nil
	cp.FieldConstraints = append(cp.FieldConstraints, ti.FieldConstraints...)
	return &cp
}

// IndexInfo holds the configuration of an index.
type IndexInfo struct {
	TableName string
	// name of the store associated with the index.
	StoreName []byte
	IndexName string
	Paths     []document.Path

	// If set to true, values will be associated with at most one key. False by default.
	Unique bool

	// If set, the index is typed and only accepts values of those types.
	Types []types.ValueType

	// If set, this index has been created from a table constraint
	// i.e CREATE TABLE tbl(a INT UNIQUE)
	// The path refers to the path this index is related to.
	Owner Owner
}

func (i *IndexInfo) Type() string {
	return "index"
}

func (i *IndexInfo) Name() string {
	return i.IndexName
}

func (i *IndexInfo) SetName(name string) {
	i.IndexName = name
}

func pathsToIndexName(paths []document.Path) string {
	var s strings.Builder

	for i, p := range paths {
		if i > 0 {
			s.WriteRune('_')
		}

		s.WriteString(p.String())
	}

	return s.String()
}

func (i *IndexInfo) GenerateBaseName() string {
	return stringutil.Sprintf("%s_%s_idx", i.TableName, pathsToIndexName(i.Paths))
}

// String returns a SQL representation.
func (i *IndexInfo) String() string {
	var s strings.Builder

	s.WriteString("CREATE ")
	if i.Unique {
		s.WriteString("UNIQUE ")
	}

	stringutil.Fprintf(&s, "INDEX %s ON %s (", stringutil.NormalizeIdentifier(i.IndexName, '`'), stringutil.NormalizeIdentifier(i.TableName, '`'))

	for i, p := range i.Paths {
		if i > 0 {
			s.WriteString(", ")
		}

		// Path
		s.WriteString(p.String())
	}

	s.WriteString(")")

	return s.String()
}

// Clone returns a copy of the index information.
func (i IndexInfo) Clone() *IndexInfo {
	c := i

	c.Paths = make([]document.Path, len(i.Paths))
	for i, p := range i.Paths {
		c.Paths[i] = p.Clone()
	}

	c.Types = make([]types.ValueType, len(i.Types))
	copy(c.Types, i.Types)

	return &c
}

// SequenceInfo holds the configuration of a sequence.
type SequenceInfo struct {
	Name        string
	IncrementBy int64
	Min, Max    int64
	Start       int64
	Cache       uint64
	Cycle       bool
	Owner       Owner
}

// String returns a SQL representation.
func (s *SequenceInfo) String() string {
	var b strings.Builder

	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(stringutil.NormalizeIdentifier(s.Name, '`'))

	asc := s.IncrementBy > 0

	if s.IncrementBy != 1 {
		stringutil.Fprintf(&b, " INCREMENT BY %d", s.IncrementBy)
	}

	if (asc && s.Min != 1) || (!asc && s.Min != math.MinInt64) {
		stringutil.Fprintf(&b, " MINVALUE %d", s.Min)
	}

	if (asc && s.Max != math.MaxInt64) || (!asc && s.Max != -1) {
		stringutil.Fprintf(&b, " MAXVALUE %d", s.Max)
	}

	if (asc && s.Start != s.Min) || (!asc && s.Start != s.Max) {
		stringutil.Fprintf(&b, " START WITH %d", s.Start)
	}

	if s.Cache != 1 {
		stringutil.Fprintf(&b, " CACHE %d", s.Cache)
	}

	if s.Cycle {
		b.WriteString(" CYCLE")
	}

	return b.String()
}

// Clone returns a copy of the sequence information.
func (s SequenceInfo) Clone() *SequenceInfo {
	return &s
}

// Owner is used to determine who owns a relation.
// If the relation has been created by a table (for docids for example),
// only the TableName is filled.
// If it has been created by a field constraint (for identities for example), the
// path must also be filled.
type Owner struct {
	TableName string
	Path      document.Path
}
