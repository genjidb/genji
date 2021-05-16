package main

import (
	"database/sql"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Note: code adapted from gorm source: go-gorm/sqlite.

// Dialector implements the Dialector interface from gorm.
type Dialector struct {
	db *sql.DB
}

// NewDialector constructs a new dialector from a sql store.
func NewDialector(db *sql.DB) gorm.Dialector {
	return &Dialector{db: db}
}

// Name returns the name of the dialector.
func (d *Dialector) Name() string {
	return "genjidb"
}

// Initialize initializes the dialector with a db.
func (d *Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		LastInsertIDReversed: true,
	})
	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return nil
}

// ClauseBuilders returns the set of clause builders.
func (d *Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if insert, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					stmt.WriteString("INSERT ")
					if insert.Modifier != "" {
						stmt.WriteString(insert.Modifier)
						stmt.WriteByte(' ')
					}

					stmt.WriteString("INTO ")
					if insert.Table.Name == "" {
						stmt.WriteQuoted(stmt.Table)
					} else {
						stmt.WriteQuoted(insert.Table)
					}
					return
				}
			}

			c.Build(builder)
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				if limit.Limit > 0 {
					builder.WriteString("LIMIT ")
					builder.WriteString(strconv.Itoa(limit.Limit))
				}
				if limit.Offset > 0 {
					if limit.Limit > 0 {
						builder.WriteString(" ")
					}
					builder.WriteString("OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
		"FOR": func(c clause.Clause, builder clause.Builder) {
			if _, ok := c.Expression.(clause.Locking); ok {
				// SQLite3 does not support row-level locking.
				return
			}
			c.Build(builder)
		},
	}
}

func (d *Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.AutoIncrement {
		return clause.Expr{SQL: "NULL"}
	}

	// doesn't work, will raise error
	return clause.Expr{SQL: "DEFAULT"}
}

func (d *Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   d,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (d *Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (d *Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".`")
			}
			writer.WriteString(str)
			writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('`')
	}
}

func (d *Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

func (d *Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "numeric"
	case schema.Int, schema.Uint:
		/*
			if field.AutoIncrement && !field.PrimaryKey {
				// https://www.sqlite.org/autoinc.html
				return "integer PRIMARY KEY AUTOINCREMENT"
			} else {
			}
		*/
		return "integer"
	case schema.Float:
		return "real"
	case schema.String:
		return "text"
	case schema.Time:
		// GenjiDB does not (yet) support datetime
		// return "datetime"
		return "text"
	case schema.Bytes:
		return "blob"
	}

	return string(field.DataType)
}

func (d *Dialector) SavePoint(tx *gorm.DB, name string) error {
	// tx.Exec("SAVEPOINT " + name)
	// return nil
	return gorm.ErrNotImplemented
}

func (d *Dialector) RollbackTo(tx *gorm.DB, name string) error {
	// tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	// return nil
	return gorm.ErrNotImplemented
}

// _ is a type assertion
var _ gorm.Dialector = ((*Dialector)(nil))
