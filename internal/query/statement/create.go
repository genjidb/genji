package statement

import (
	"math"

	"github.com/genjidb/genji/document"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/types"
)

// CreateTableStmt represents a parsed CREATE TABLE statement.
type CreateTableStmt struct {
	IfNotExists bool
	Info        database.TableInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *CreateTableStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create table statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateTableStmt) Run(ctx *Context) (Result, error) {
	var res Result

	// if there is no primary key, create a docid sequence
	if stmt.Info.FieldConstraints.GetPrimaryKey() == nil {
		seq := database.SequenceInfo{
			IncrementBy: 1,
			Min:         1, Max: math.MaxInt64,
			Start: 1,
			Cache: 64,
			Owner: database.Owner{
				TableName: stmt.Info.TableName,
			},
		}
		err := ctx.Catalog.CreateSequence(ctx.Tx, &seq)
		if err != nil {
			return res, err
		}

		stmt.Info.DocidSequenceName = seq.Name
	}

	err := ctx.Catalog.CreateTable(ctx.Tx, stmt.Info.TableName, &stmt.Info)
	if stmt.IfNotExists {
		if _, ok := err.(errs.AlreadyExistsError); ok {
			return res, nil
		}
	}

	// create a unique index for every unique constraint
	for _, fc := range stmt.Info.FieldConstraints {
		if fc.IsUnique {
			err = ctx.Catalog.CreateIndex(ctx.Tx, &database.IndexInfo{
				TableName: stmt.Info.TableName,
				Paths:     []document.Path{fc.Path},
				Unique:    true,
				Types:     []types.ValueType{fc.Type},
				Owner: database.Owner{
					TableName: stmt.Info.TableName,
					Path:      fc.Path,
				},
			})
			if err != nil {
				return res, err
			}
		}
	}

	return res, err
}

// CreateIndexStmt represents a parsed CREATE INDEX statement.
type CreateIndexStmt struct {
	IfNotExists bool
	Info        database.IndexInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *CreateIndexStmt) IsReadOnly() bool {
	return false
}

// Run runs the Create index statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateIndexStmt) Run(ctx *Context) (Result, error) {
	var res Result

	err := ctx.Catalog.CreateIndex(ctx.Tx, &stmt.Info)
	if stmt.IfNotExists {
		if _, ok := err.(errs.AlreadyExistsError); ok {
			return res, nil
		}
	}
	if err != nil {
		return res, err
	}

	err = ctx.Catalog.ReIndex(ctx.Tx, stmt.Info.IndexName)
	return res, err
}

// CreateSequenceStmt represents a parsed CREATE SEQUENCE statement.
type CreateSequenceStmt struct {
	IfNotExists bool
	Info        database.SequenceInfo
}

// IsReadOnly always returns false. It implements the Statement interface.
func (stmt *CreateSequenceStmt) IsReadOnly() bool {
	return false
}

// Run the statement in the given transaction.
// It implements the Statement interface.
func (stmt *CreateSequenceStmt) Run(ctx *Context) (Result, error) {
	var res Result

	err := ctx.Catalog.CreateSequence(ctx.Tx, &stmt.Info)
	if stmt.IfNotExists {
		if _, ok := err.(errs.AlreadyExistsError); ok {
			return res, nil
		}
	}
	return res, err
}
