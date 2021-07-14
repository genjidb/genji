package catalog

import (
	"strings"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stringutil"
)

func relationToDocument(r Relation) document.Document {
	switch t := r.(type) {
	case *database.TableInfo:
		return tableInfoToDocument(t)
	case *database.IndexInfo:
		return indexInfoToDocument(t)
	case *database.Sequence:
		return sequenceInfoToDocument(t.Info)
	}

	panic(stringutil.Sprintf("objectToDocument: unknown type %q", r.Type()))
}

func tableInfoToDocument(ti *database.TableInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(ti.TableName))
	buf.Add("type", document.NewTextValue(RelationTableType))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	buf.Add("sql", document.NewTextValue(ti.String()))
	if ti.DocidSequenceName != "" {
		buf.Add("docid_sequence_name", document.NewTextValue(ti.DocidSequenceName))
	}

	return buf
}

func tableInfoFromDocument(d document.Document) (*database.TableInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	ti := stmt.(*statement.CreateTableStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	ti.StoreName = v.V().([]byte)

	v, err = d.GetByField("docid_sequence_name")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		ti.DocidSequenceName = v.V().(string)
	}

	return &ti, nil
}

func indexInfoToDocument(i *database.IndexInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(i.IndexName))
	buf.Add("type", document.NewTextValue(RelationIndexType))
	buf.Add("store_name", document.NewBlobValue(i.StoreName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("sql", document.NewTextValue(i.String()))
	if i.Owner.TableName != "" {
		buf.Add("owner", document.NewDocumentValue(ownerToDocument(&i.Owner)))
	}

	return buf
}

func indexInfoFromDocument(d document.Document) (*database.IndexInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateIndexStmt).Info

	v, err := d.GetByField("store_name")
	if err != nil {
		return nil, err
	}
	i.StoreName = v.V().([]byte)

	v, err = d.GetByField("owner")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		owner, err := ownerFromDocument(v.V().(document.Document))
		if err != nil {
			return nil, err
		}
		i.Owner = *owner
	}

	return &i, nil
}

func sequenceInfoToDocument(seq *database.SequenceInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(seq.Name))
	buf.Add("type", document.NewTextValue(RelationSequenceType))
	buf.Add("sql", document.NewTextValue(seq.String()))

	if seq.Owner.TableName != "" {
		owner := document.NewFieldBuffer().Add("table_name", document.NewTextValue(seq.Owner.TableName))
		if seq.Owner.Path != nil {
			owner.Add("path", document.NewTextValue(seq.Owner.Path.String()))
		}

		buf.Add("owner", document.NewDocumentValue(owner))
	}

	return buf
}

func sequenceInfoFromDocument(d document.Document) (*database.SequenceInfo, error) {
	s, err := d.GetByField("sql")
	if err != nil {
		return nil, err
	}

	stmt, err := parser.NewParser(strings.NewReader(s.V().(string))).ParseStatement()
	if err != nil {
		return nil, err
	}

	i := stmt.(*statement.CreateSequenceStmt).Info

	v, err := d.GetByField("owner")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		owner, err := ownerFromDocument(v.V().(document.Document))
		if err != nil {
			return nil, err
		}
		i.Owner = *owner
	}

	return &i, nil
}

func ownerToDocument(owner *database.Owner) document.Document {
	buf := document.NewFieldBuffer().Add("table_name", document.NewTextValue(owner.TableName))
	if owner.Path != nil {
		buf.Add("path", document.NewTextValue(owner.Path.String()))
	}

	return buf
}

func ownerFromDocument(d document.Document) (*database.Owner, error) {
	var owner database.Owner

	v, err := d.GetByField("table_name")
	if err != nil {
		return nil, err
	}

	owner.TableName = v.V().(string)

	v, err = d.GetByField("path")
	if err != nil && err != document.ErrFieldNotFound {
		return nil, err
	}
	if err == nil {
		owner.Path, err = parser.ParsePath(v.V().(string))
		if err != nil {
			return nil, err
		}
	}

	return &owner, nil
}

type CatalogTable struct {
	Catalog *Catalog
	Info    *database.TableInfo
}

func NewCatalogTable(tx *database.Transaction, catalog *Catalog) *CatalogTable {
	return &CatalogTable{
		Catalog: catalog,
		Info: &database.TableInfo{
			TableName: TableName,
			StoreName: []byte(TableName),
			FieldConstraints: []*database.FieldConstraint{
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "name",
						},
					},
					Type:         document.TextValue,
					IsPrimaryKey: true,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "type",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "table_name",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "sql",
						},
					},
					Type: document.TextValue,
				},
				{
					Path: document.Path{
						document.PathFragment{
							FieldName: "store_name",
						},
					},
					Type: document.BlobValue,
				},
			},
		},
	}
}

func (s *CatalogTable) Init(tx *database.Transaction) error {
	_, err := tx.Tx.GetStore([]byte(TableName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(TableName))
	}

	return err
}

func (s *CatalogTable) Load(tx *database.Transaction) (tables []database.TableInfo, indexes []database.IndexInfo, sequences []database.SequenceInfo, err error) {
	tb := s.Table(tx)

	err = tb.AscendGreaterOrEqual(nil, func(d document.Document) error {
		tp, err := d.GetByField("type")
		if err != nil {
			return err
		}

		switch tp.V().(string) {
		case RelationTableType:
			ti, err := tableInfoFromDocument(d)
			if err != nil {
				return err
			}
			tables = append(tables, *ti)
		case RelationIndexType:
			i, err := indexInfoFromDocument(d)
			if err != nil {
				return err
			}

			indexes = append(indexes, *i)
		case RelationSequenceType:
			i, err := sequenceInfoFromDocument(d)
			if err != nil {
				return err
			}
			sequences = append(sequences, *i)
		}

		return nil
	})
	return
}

func (s *CatalogTable) Table(tx *database.Transaction) *database.Table {
	st, err := tx.Tx.GetStore([]byte(TableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", TableName, err))
	}

	return &database.Table{
		Tx:      tx,
		Store:   st,
		Info:    s.Info,
		Catalog: s.Catalog,
	}
}

// Insert a catalog object to the table.
func (s *CatalogTable) Insert(tx *database.Transaction, r Relation) error {
	tb := s.Table(tx)

	_, err := tb.Insert(relationToDocument(r))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: r.Name()}
	}

	return err
}

// Replace a catalog object with another.
func (s *CatalogTable) Replace(tx *database.Transaction, name string, r Relation) error {
	tb := s.Table(tx)

	_, err := tb.Replace([]byte(name), relationToDocument(r))
	return err
}

func (s *CatalogTable) Delete(tx *database.Transaction, name string) error {
	tb := s.Table(tx)

	return tb.Delete([]byte(name))
}
