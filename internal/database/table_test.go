package database_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/document/encoding/msgpack"
	"github.com/genjidb/genji/engine/memoryengine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/binarysort"
	"github.com/genjidb/genji/internal/catalog"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

var errDontCommit = errors.New("don't commit please")

func update(t testing.TB, db *database.Database, fn func(tx *database.Transaction) error) {
	t.Helper()

	tx, err := db.Begin(true)
	require.NoError(t, err)
	defer tx.Rollback()

	err = fn(tx)
	if err == errDontCommit {
		tx.Rollback()
		return
	}
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func newTestTable(t testing.TB) (*database.Table, func()) {
	db, tx, fn := testutil.NewTestTx(t)

	return createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test"}), fn
}

func createTable(t testing.TB, tx *database.Transaction, catalog database.Catalog, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	require.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	require.NoError(t, err)

	return tb
}

func createTableIfNotExists(t testing.TB, tx *database.Transaction, catalog database.Catalog, info database.TableInfo) *database.Table {
	stmt := statement.CreateTableStmt{Info: info, IfNotExists: true}

	res, err := stmt.Run(&statement.Context{
		Catalog: catalog,
		Tx:      tx,
	})
	require.NoError(t, err)
	res.Close()

	tb, err := catalog.GetTable(tx, stmt.Info.TableName)
	require.NoError(t, err)

	return tb
}

func newDocument() *document.FieldBuffer {
	return document.NewFieldBuffer().
		Add("fielda", document.NewTextValue("a")).
		Add("fieldb", document.NewTextValue("b"))
}

// TestTableIterate verifies Iterate behaviour.
func TestTableIterate(t *testing.T) {
	t.Run("Should not fail with no documents", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		i := 0
		err := tb.Iterate(func(d document.Document) error {
			i++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, i)
	})

	t.Run("Should iterate over all documents", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newDocument())
			require.NoError(t, err)
		}

		m := make(map[string]int)
		err := tb.Iterate(func(d document.Document) error {
			m[string(d.(document.Keyer).RawKey())]++
			return nil
		})
		require.NoError(t, err)
		require.Len(t, m, 10)
		for _, c := range m {
			require.Equal(t, 1, c)
		}
	})

	t.Run("Should stop if fn returns error", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		for i := 0; i < 10; i++ {
			_, err := tb.Insert(newDocument())
			require.NoError(t, err)
		}

		i := 0
		err := tb.Iterate(func(_ document.Document) error {
			i++
			if i >= 5 {
				return errors.New("some error")
			}
			return nil
		})
		require.EqualError(t, err, "some error")
		require.Equal(t, 5, i)
	})
}

// TestTableGetDocument verifies GetDocument behaviour.
func TestTableGetDocument(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		r, err := tb.GetDocument([]byte("id"))
		require.Equal(t, errs.ErrDocumentNotFound, err)
		require.Nil(t, r)
	})

	t.Run("Should return the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		vc := document.NewDoubleValue(40)
		doc1.Add("fieldc", vc)
		doc2 := newDocument()

		d, err := tb.Insert(doc1)
		require.NoError(t, err)
		_, err = tb.Insert(doc2)
		require.NoError(t, err)

		// fetch doc1 and make sure it returns the right one
		res, err := tb.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)
		fc, err := res.GetByField("fieldc")
		require.NoError(t, err)
		require.Equal(t, vc, fc)
	})
}

// TestTableInsert verifies Insert behaviour.
func TestTableInsert(t *testing.T) {
	t.Run("Should generate a key by default", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		doc := newDocument()
		d1, err := tb.Insert(doc.Clone())
		require.NoError(t, err)
		require.NotEmpty(t, d1.(document.Keyer).RawKey())

		d2, err := tb.Insert(doc.Clone())
		require.NoError(t, err)
		require.NotEmpty(t, d2.(document.Keyer).RawKey())

		require.NotEqual(t, d1.(document.Keyer).RawKey(), d2.(document.Keyer).RawKey())
	})

	t.Run("Should generate the right docid on existing databases", func(t *testing.T) {
		ng := memoryengine.NewEngine()

		db, err := database.New(context.Background(), ng, database.Options{
			Codec:   msgpack.NewCodec(),
			Catalog: catalog.New(),
		})
		require.NoError(t, err)

		insertDoc := func(db *database.Database) (rawKey []byte) {
			update(t, db, func(tx *database.Transaction) error {
				// create table if not exists
				tb := createTableIfNotExists(t, tx, db.Catalog, database.TableInfo{TableName: "test"})

				doc := newDocument()
				d, err := tb.Insert(doc)
				require.NoError(t, err)
				require.NotEmpty(t, d.(document.Keyer).RawKey())
				rawKey = d.(document.Keyer).RawKey()
				return nil
			})
			return
		}

		key1 := insertDoc(db)

		err = db.Close()
		require.NoError(t, err)

		ng.Closed = false

		// create new database object
		db, err = database.New(context.Background(), ng, database.Options{
			Codec:   msgpack.NewCodec(),
			Catalog: db.Catalog,
		})

		require.NoError(t, err)

		key2 := insertDoc(db)

		a, _ := binary.Uvarint(key1)
		require.NoError(t, err)

		b, _ := binary.Uvarint(key2)
		require.NoError(t, err)

		require.Equal(t, int64(a+1), int64(b))
	})

	t.Run("Should use the right field if primary key is specified", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo.a[1]"), Type: types.IntegerValue, IsPrimaryKey: true},
			},
		})
		require.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		var doc document.FieldBuffer
		err = doc.UnmarshalJSON([]byte(`{"foo": {"a": [0, 10]}}`))
		require.NoError(t, err)

		// insert
		d, err := tb.Insert(doc)
		require.NoError(t, err)
		require.Equal(t, binarysort.AppendInt64(nil, 10), d.(document.Keyer).RawKey())

		// make sure the document is fetchable using the returned key
		_, err = tb.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)

		// insert again
		_, err = tb.Insert(doc)
		require.Equal(t, errs.ErrDuplicateDocument, err)
	})

	t.Run("Should convert values into the right types if there are constraints", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.ArrayValue},
				{Path: testutil.ParseDocumentPath(t, "foo[0]"), Type: types.IntegerValue},
			},
		})

		var doc document.FieldBuffer
		err := doc.UnmarshalJSON([]byte(`{"foo": [100]}`))
		require.NoError(t, err)

		// insert
		d, err := tb.Insert(doc)
		require.NoError(t, err)

		d, err = tb.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)

		v, err := testutil.ParseDocumentPath(t, "foo[0]").GetValueFromDocument(d)
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(100), v)
	})

	t.Run("Should fail if Pk not found in document or empty", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue, IsPrimaryKey: true},
			},
		})
		require.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		tests := [][]byte{
			nil,
			{},
			[]byte(nil),
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%#v", test), func(t *testing.T) {
				doc := document.NewFieldBuffer().
					Add("foo", document.NewBlobValue(test))

				_, err := tb.Insert(doc)
				require.Error(t, err)
			})
		}
	})

	t.Run("Should update indexes if there are indexed fields", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test"})

		err := db.Catalog.CreateIndex(tx, &database.IndexInfo{
			IndexName: "idxFoo", TableName: "test", Paths: []document.Path{testutil.ParseDocumentPath(t, "foo")},
		})
		require.NoError(t, err)
		idx, err := db.Catalog.GetIndex(tx, "idxFoo")
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		// create one document with the foo field
		doc1 := newDocument()
		foo := document.NewDoubleValue(10)
		doc1.Add("foo", foo)

		// create one document without the foo field
		doc2 := newDocument()

		d1, err := tb.Insert(doc1)
		require.NoError(t, err)
		d2, err := tb.Insert(doc2)
		require.NoError(t, err)

		var count int
		err = idx.AscendGreaterOrEqual([]document.Value{nil}, func(val, k []byte) error {
			switch count {
			case 0:
				// key2, which doesn't countain the field must appear first in the next,
				// as null values are the smallest possible values
				require.Equal(t, d2.(document.Keyer).RawKey(), k)
			case 1:
				require.Equal(t, d1.(document.Keyer).RawKey(), k)
			}
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})

	t.Run("Should convert the fields if FieldsConstraints are specified", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DocumentValue, IsInferred: true, InferredBy: []document.Path{testutil.ParseDocumentPath(t, "foo.bar")}},
				{Path: testutil.ParseDocumentPath(t, "foo.bar"), Type: types.IntegerValue, IsInferred: true, InferredBy: []document.Path{testutil.ParseDocumentPath(t, "foo")}},
			},
		})

		doc := document.NewFieldBuffer().
			Add("foo", document.NewDocumentValue(
				document.NewFieldBuffer().Add("bar", document.NewDoubleValue(10)),
			)).
			Add("bar", document.NewDoubleValue(10)).
			Add("baz", document.NewTextValue("baz")).
			Add("bat", document.NewIntegerValue(20))

		// insert
		d, err := tb.Insert(doc)
		require.NoError(t, err)

		// make sure the fields have been converted to the right types
		d, err = tb.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)
		v, err := d.GetByField("foo")
		require.NoError(t, err)
		v, err = v.V().(document.Document).GetByField("bar")
		require.NoError(t, err)
		require.Equal(t, document.NewIntegerValue(10), v)
		v, err = d.GetByField("bar")
		require.NoError(t, err)
		require.Equal(t, document.NewDoubleValue(10), v)
		v, err = d.GetByField("baz")
		require.NoError(t, err)
		require.Equal(t, document.NewTextValue("baz"), v)
		v, err = d.GetByField("bat")
		require.NoError(t, err)
		require.Equal(t, document.NewDoubleValue(20), v)
	})

	t.Run("Should fail if the fields cannot be converted to specified field constraints", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.DoubleValue},
			},
		})
		require.NoError(t, err)
		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("foo", document.NewArrayValue(document.NewValueBuffer(document.NewIntegerValue(1))))

		// insert
		_, err = tb.Insert(doc)
		require.Error(t, err)
	})

	t.Run("Should fail if there is a not null field constraint on a document field and the field is null or missing", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		// no enforced type, not null
		tb1 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true},
			},
		})

		// enforced type, not null
		tb2 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test2",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue, IsNotNull: true},
			},
		})

		// insert with empty foo field should fail
		_, err := tb1.Insert(document.NewFieldBuffer().
			Add("bar", document.NewDoubleValue(1)))
		require.Error(t, err)

		// insert with null foo field should fail
		_, err = tb1.Insert(document.NewFieldBuffer().
			Add("foo", document.NewNullValue()))
		require.Error(t, err)

		// otherwise it should work
		_, err = tb1.Insert(document.NewFieldBuffer().
			Add("foo", document.NewDoubleValue(1)))
		require.NoError(t, err)

		// insert with empty foo field should fail
		_, err = tb2.Insert(document.NewFieldBuffer().
			Add("bar", document.NewDoubleValue(1)))
		require.Error(t, err)

		// insert with null foo field should fail
		_, err = tb2.Insert(document.NewFieldBuffer().
			Add("foo", document.NewNullValue()))
		require.Error(t, err)

		// otherwise it should work
		_, err = tb2.Insert(document.NewFieldBuffer().
			Add("foo", document.NewDoubleValue(1)))
		require.NoError(t, err)
	})

	t.Run("Shouldn't fail if there is a not null field and default constraint on a document field and the field is null or missing", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		// no enforced type, not null
		tb1 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true, DefaultValue: expr.Constraint(testutil.IntegerValue(42))},
			},
		})

		// enforced type, not null
		tb2 := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test2",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), Type: types.IntegerValue, IsNotNull: true, DefaultValue: expr.Constraint(testutil.IntegerValue(42))},
			},
		})

		// insert with empty foo field shouldn't fail
		d, err := tb1.Insert(document.NewFieldBuffer().
			Add("bar", document.NewDoubleValue(1)))
		require.NoError(t, err)

		d, err = tb1.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)
		v, err := d.GetByField("foo")
		require.NoError(t, err)
		require.Equal(t, v.V().(float64), float64(42))

		// insert with explicit null foo field should fail
		_, err = tb1.Insert(document.NewFieldBuffer().
			Add("foo", document.NewNullValue()))
		require.Error(t, err)

		// otherwise it should work
		_, err = tb1.Insert(document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(1)))
		require.NoError(t, err)

		// insert with empty foo field shouldn't fail
		d, err = tb2.Insert(document.NewFieldBuffer().
			Add("bar", document.NewIntegerValue(1)))
		require.NoError(t, err)

		d, err = tb2.GetDocument(d.(document.Keyer).RawKey())
		require.NoError(t, err)
		v, err = d.GetByField("foo")
		require.NoError(t, err)
		require.Equal(t, v.V().(int64), int64(42))

		// insert with explicit null foo field should fail
		_, err = tb2.Insert(document.NewFieldBuffer().
			Add("foo", document.NewNullValue()))
		require.Error(t, err)

		// otherwise it should work
		_, err = tb2.Insert(document.NewFieldBuffer().
			Add("foo", document.NewDoubleValue(1)))
		require.NoError(t, err)
	})

	t.Run("Should fail if there is a not null field constraint on an array value and the value is null", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test1",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo[1]"), IsNotNull: true},
			},
		})

		// insert table with only one value
		_, err := tb.Insert(document.NewFieldBuffer().
			Add("foo", document.NewArrayValue(document.NewValueBuffer().Append(document.NewIntegerValue(1)))))
		require.Error(t, err)
		_, err = tb.Insert(document.NewFieldBuffer().
			Add("foo", document.NewArrayValue(document.NewValueBuffer().
				Append(document.NewIntegerValue(1)).Append(document.NewIntegerValue(2)))))
		require.NoError(t, err)
	})

	t.Run("Should fail if the pk is duplicated", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsPrimaryKey: true, IsNotNull: true},
			}})

		doc := document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(10))

		// insert first
		_, err := tb.Insert(doc)
		require.NoError(t, err)

		// insert again, should fail
		_, err = tb.Insert(doc)
		require.Equal(t, errs.ErrDuplicateDocument, err)
	})

	t.Run("Should run the onConflict function if the pk is duplicated", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsPrimaryKey: true, IsNotNull: true},
			}})

		doc := document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(10))

		var called int
		onConflict := func(t *database.Table, key []byte, d document.Document, err error) (document.Document, error) {
			called++
			return d, nil
		}

		// insert first
		_, err := tb.InsertWithConflictResolution(doc, onConflict)
		require.NoError(t, err)
		require.Equal(t, 0, called)

		// insert again, should call onConflict
		_, err = tb.InsertWithConflictResolution(doc, onConflict)
		require.NoError(t, err)
		require.Equal(t, 1, called)
	})

	t.Run("Should fail if there is a unique constraint violation", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test"})
		err := db.Catalog.CreateIndex(tx, &database.IndexInfo{
			TableName: "test",
			IndexName: "idx_test_foo",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "foo")},
			Unique:    true,
		})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(10))

		// insert first
		_, err = tb.Insert(doc)
		require.NoError(t, err)

		// insert again, should fail
		_, err = tb.Insert(doc)
		require.Equal(t, errs.ErrDuplicateDocument, err)
	})

	t.Run("Should run the onConflict function if there is a unique constraint violation", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test"})
		err := db.Catalog.CreateIndex(tx, &database.IndexInfo{
			TableName: "test",
			IndexName: "idx_test_foo",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "foo")},
			Unique:    true,
		})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(10))

		var called int
		onConflict := func(t *database.Table, key []byte, d document.Document, err error) (document.Document, error) {
			called++
			return d, nil
		}

		// insert first
		_, err = tb.InsertWithConflictResolution(doc, onConflict)
		require.NoError(t, err)
		require.Equal(t, 0, called)

		// insert again, should call onConflict
		_, err = tb.InsertWithConflictResolution(doc, onConflict)
		require.NoError(t, err)
		require.Equal(t, 1, called)
	})

	t.Run("Should run the onConflict function if there is a NOT NULL constraint violation", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		tb := createTable(t, tx, db.Catalog, database.TableInfo{
			TableName: "test",
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true},
			},
		})

		doc := document.NewFieldBuffer().
			Add("bar", document.NewIntegerValue(10))

		var called int
		onConflict := func(t *database.Table, key []byte, d document.Document, err error) (document.Document, error) {
			called++
			return d, nil
		}

		// insert
		_, err := tb.InsertWithConflictResolution(doc, onConflict)
		require.NoError(t, err)
		require.Equal(t, 1, called)
	})

	t.Run("Should replace document if the pk is duplicated, using OnInsertConflictDoReplace", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsPrimaryKey: true, IsNotNull: true},
			}})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("foo", document.NewIntegerValue(10))

		// insert first
		d1, err := tb.Insert(doc)
		require.NoError(t, err)

		// insert again, should call OnInsertConflictDoReplace
		d2, err := tb.InsertWithConflictResolution(doc, database.OnInsertConflictDoReplace)
		require.NoError(t, err)
		require.Equal(t, d1, d2)
	})

	t.Run("Should not replace document  if there is a NOT NULL constraint violation, using OnInsertConflictDoReplace", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test", &database.TableInfo{
			FieldConstraints: []*database.FieldConstraint{
				{Path: testutil.ParseDocumentPath(t, "foo"), IsNotNull: true},
			}})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test")
		require.NoError(t, err)

		doc := document.NewFieldBuffer().
			Add("bar", document.NewIntegerValue(10))

		// insert
		_, err = tb.InsertWithConflictResolution(doc, database.OnInsertConflictDoReplace)
		require.Error(t, err)
	})
}

// TestTableDelete verifies Delete behaviour.
func TestTableDelete(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Delete([]byte("id"))
		require.Equal(t, errs.ErrDocumentNotFound, err)
	})

	t.Run("Should delete the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents, one with an additional field
		doc1 := newDocument()
		doc1.Add("fieldc", document.NewIntegerValue(40))
		doc2 := newDocument()

		d1, err := tb.Insert(doc1.Clone())
		require.NoError(t, err)
		d2, err := tb.Insert(doc2.Clone())
		require.NoError(t, err)

		// delete the document
		err = tb.Delete([]byte(d1.(document.Keyer).RawKey()))
		require.NoError(t, err)

		// try again, should fail
		err = tb.Delete([]byte(d1.(document.Keyer).RawKey()))
		require.Equal(t, errs.ErrDocumentNotFound, err)

		// make sure it didn't also delete the other one
		res, err := tb.GetDocument(d2.(document.Keyer).RawKey())
		require.NoError(t, err)
		_, err = res.GetByField("fieldc")
		require.Error(t, err)
	})
}

// TestTableReplace verifies Replace behaviour.
func TestTableReplace(t *testing.T) {
	t.Run("Should fail if not found", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		_, err := tb.Replace([]byte("id"), newDocument())
		require.Equal(t, errs.ErrDocumentNotFound, err)
	})

	t.Run("Should replace the right document", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two different documents
		doc1 := newDocument()
		doc2 := document.NewFieldBuffer().
			Add("fielda", document.NewTextValue("c")).
			Add("fieldb", document.NewTextValue("d"))

		d1, err := tb.Insert(doc1)
		require.NoError(t, err)
		d2, err := tb.Insert(doc2)
		require.NoError(t, err)

		// create a third document
		doc3 := document.NewFieldBuffer().
			Add("fielda", document.NewTextValue("e")).
			Add("fieldb", document.NewTextValue("f"))

		// replace doc1 with doc3
		d3, err := tb.Replace(d1.(document.Keyer).RawKey(), doc3)
		require.NoError(t, err)

		// make sure it replaced it correctly
		res, err := tb.GetDocument(d1.(document.Keyer).RawKey())
		require.NoError(t, err)
		f, err := res.GetByField("fielda")
		require.NoError(t, err)
		require.Equal(t, "e", f.V().(string))

		testutil.RequireDocEqual(t, d3, res)

		// make sure it didn't also replace the other one
		res, err = tb.GetDocument(d2.(document.Keyer).RawKey())
		require.NoError(t, err)
		f, err = res.GetByField("fielda")
		require.NoError(t, err)
		require.Equal(t, "c", f.V().(string))
	})

	t.Run("Should update indexes", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test1"})
		createTable(t, tx, db.Catalog, database.TableInfo{TableName: "test2"})

		// simple indexes
		err := db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Paths:     []document.Path{document.NewPath("a")},
			Unique:    true,
			TableName: "test1",
			IndexName: "idx_foo_a",
		})
		require.NoError(t, err)

		// composite indexes
		err = db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Paths:     []document.Path{document.NewPath("x"), document.NewPath("y")},
			Unique:    true,
			TableName: "test2",
			IndexName: "idx_foo_x_y",
		})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test1")
		require.NoError(t, err)

		// insert two different documents
		d1, err := tb.Insert(testutil.MakeDocument(t, `{"a": 1, "b": 1}`))
		require.NoError(t, err)
		d2, err := tb.Insert(testutil.MakeDocument(t, `{"a": 2, "b": 2}`))
		require.NoError(t, err)

		beforeIdxA := testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_a")

		// --- a
		// replace d1 without modifying indexed key
		_, err = tb.Replace(d1.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"a": 1, "b": 3}`))
		require.NoError(t, err)

		// indexes should be the same as before
		require.Equal(t, beforeIdxA, testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_a"))

		// replace d2 and modify indexed key
		_, err = tb.Replace(d2.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"a": 3, "b": 3}`))
		require.NoError(t, err)

		// indexes should be different for d2
		got := testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_a")
		require.Equal(t, beforeIdxA[0], got[0])
		require.NotEqual(t, beforeIdxA[1], got[1])

		// replace d1 with duplicate indexed key
		_, err = tb.Replace(d1.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"a": 3, "b": 3}`))

		// index should be the same as before
		require.Equal(t, errs.ErrDuplicateDocument, err)

		// --- x, y
		tb, err = db.Catalog.GetTable(tx, "test2")
		require.NoError(t, err)
		// insert two different documents
		dc1, err := tb.Insert(testutil.MakeDocument(t, `{"x": 1, "y": 1, "z": 1}`))
		require.NoError(t, err)
		dc2, err := tb.Insert(testutil.MakeDocument(t, `{"x": 2, "y": 2, "z": 2}`))
		require.NoError(t, err)

		beforeIdxXY := testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_x_y")
		// replace dc1 without modifying indexed key
		_, err = tb.Replace(dc1.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"x": 1, "y": 1, "z": 2}`))
		require.NoError(t, err)

		// index should be the same as before
		require.Equal(t, beforeIdxXY, testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_x_y"))

		// replace dc2 and modify indexed key
		_, err = tb.Replace(dc2.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"x": 3, "y": 3, "z": 3}`))
		require.NoError(t, err)

		// indexes should be different for d2
		got = testutil.GetIndexContent(t, tx, db.Catalog, "idx_foo_x_y")
		require.Equal(t, beforeIdxXY[0], got[0])
		require.NotEqual(t, beforeIdxXY[1], got[1])

		// replace dc2 with duplicate indexed key
		_, err = tb.Replace(dc1.(document.Keyer).RawKey(), testutil.MakeDocument(t, `{"x": 3, "y": 3, "z": 3}`))

		// index should be the same as before
		require.Equal(t, errs.ErrDuplicateDocument, err)
	})
}

// TestTableTruncate verifies Truncate behaviour.
func TestTableTruncate(t *testing.T) {
	t.Run("Should succeed if table empty", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		err := tb.Truncate()
		require.NoError(t, err)
	})

	t.Run("Should truncate the table", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		// create two documents
		doc1 := newDocument()
		doc2 := newDocument()

		_, err := tb.Insert(doc1)
		require.NoError(t, err)
		_, err = tb.Insert(doc2)
		require.NoError(t, err)

		err = tb.Truncate()
		require.NoError(t, err)

		err = tb.Iterate(func(_ document.Document) error {
			return errors.New("should not iterate")
		})

		require.NoError(t, err)
	})
}

func TestTableIndexes(t *testing.T) {
	t.Run("Should succeed if table has no indexes", func(t *testing.T) {
		tb, cleanup := newTestTable(t)
		defer cleanup()

		list, err := tb.GetIndexes()
		require.NoError(t, err)
		require.Empty(t, list)
	})

	t.Run("Should return a list of all the indexes", func(t *testing.T) {
		db, tx, cleanup := newTestTx(t)
		defer cleanup()

		err := db.Catalog.CreateTable(tx, "test1", nil)
		require.NoError(t, err)

		err = db.Catalog.CreateTable(tx, "test2", nil)
		require.NoError(t, err)

		err = db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Unique:    true,
			IndexName: "idx1a",
			TableName: "test1",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
		})
		require.NoError(t, err)
		err = db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Unique:    false,
			IndexName: "idx1b",
			TableName: "test1",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "b")},
		})
		require.NoError(t, err)
		err = db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Unique:    false,
			IndexName: "idx1ab",
			TableName: "test1",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "a"), testutil.ParseDocumentPath(t, "b")},
		})
		require.NoError(t, err)
		err = db.Catalog.CreateIndex(tx, &database.IndexInfo{
			Unique:    false,
			IndexName: "idx2a",
			TableName: "test2",
			Paths:     []document.Path{testutil.ParseDocumentPath(t, "a")},
		})
		require.NoError(t, err)

		tb, err := db.Catalog.GetTable(tx, "test1")
		require.NoError(t, err)

		m, err := tb.GetIndexes()
		require.NoError(t, err)
		require.Len(t, m, 3)
	})
}

// BenchmarkTableInsert benchmarks the Insert method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableInsert(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), document.NewIntegerValue(i))
			}

			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tb, cleanup := newTestTable(b)

				b.StartTimer()
				for j := 0; j < size; j++ {
					tb.Insert(&fb)
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkTableScan benchmarks the Scan method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkTableScan(b *testing.B) {
	for size := 1; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			tb, cleanup := newTestTable(b)
			defer cleanup()

			var fb document.FieldBuffer

			for i := int64(0); i < 10; i++ {
				fb.Add(fmt.Sprintf("name-%d", i), document.NewIntegerValue(i))
			}

			for i := 0; i < size; i++ {
				_, err := tb.Insert(&fb)
				require.NoError(b, err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tb.Iterate(func(document.Document) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
