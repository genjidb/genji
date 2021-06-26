package database

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	errs "github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/internal/stringutil"
)

const (
	internalPrefix = "__genji_"
)

const (
	CatalogTableName         = internalPrefix + "catalog"
	CatalogTableTableType    = "table"
	CatalogTableIndexType    = "index"
	CatalogTableSequenceType = "sequence"
)

// Catalog manages all database objects such as tables, indexes and sequences.
// It stores all these objects in memory for fast access. Any modification
// is persisted into the __genji_catalog table.
type Catalog struct {
	cache         *catalogCache
	SchemaTable   *CatalogTable
	SequenceTable *SequenceTable
}

func NewCatalog(schemaTable *CatalogTable, sequenceTable *SequenceTable) *Catalog {
	return &Catalog{
		SchemaTable:   schemaTable,
		SequenceTable: sequenceTable,
		cache:         newCatalogCache(),
	}
}

func (c *Catalog) Load(tables []TableInfo, indexes []IndexInfo, sequences []Sequence) {
	// add the __genji_catalog table to the list of tables
	// so that it can be queried
	ti := c.SchemaTable.info.Clone()
	// make sure that table is read-only
	ti.ReadOnly = true
	tables = append(tables, *ti)

	// add the __genji_sequence table to the list of tables
	// so that it can be queried.
	// That table can be modified
	tables = append(tables, *c.SequenceTable.info)

	c.cache.load(tables, indexes, sequences)
}

// Clone the catalog. Mostly used for testing purposes.
func (c *Catalog) Clone() *Catalog {
	var clone Catalog

	clone.SchemaTable = c.SchemaTable
	clone.SequenceTable = c.SequenceTable
	clone.cache = c.cache.clone()

	return &clone
}

func (c *Catalog) GetTable(tx *Transaction, tableName string) (*Table, error) {
	ti, err := c.cache.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	s, err := tx.Tx.GetStore(ti.StoreName)
	if err != nil {
		return nil, err
	}

	idxInfos := c.cache.GetTableIndexes(tableName)
	indexes := make([]*Index, 0, len(idxInfos))

	for i := range idxInfos {
		indexes = append(indexes, NewIndex(tx.Tx, idxInfos[i].IndexName, idxInfos[i]))
	}

	return &Table{
		Tx:      tx,
		Name:    tableName,
		Store:   s,
		Info:    ti,
		Indexes: indexes,
	}, nil
}

// CreateTable creates a table with the given name.
// If it already exists, returns ErrTableAlreadyExists.
func (c *Catalog) CreateTable(tx *Transaction, tableName string, info *TableInfo) error {
	if info == nil {
		info = new(TableInfo)
	}
	info.TableName = tableName

	var err error

	// replace user-defined constraints by inferred list of constraints
	info.FieldConstraints, err = info.FieldConstraints.Infer()
	if err != nil {
		return err
	}

	err = c.SchemaTable.insertTable(tx, tableName, info)
	if err != nil {
		return err
	}

	err = tx.Tx.CreateStore(info.StoreName)
	if err != nil {
		return stringutil.Errorf("failed to create table %q: %w", tableName, err)
	}

	return c.cache.AddTable(tx, info)
}

// DropTable deletes a table from the
func (c *Catalog) DropTable(tx *Transaction, tableName string) error {
	ti, removedIndexes, err := c.cache.DeleteTable(tx, tableName)
	if err != nil {
		return err
	}

	for _, idx := range removedIndexes {
		err := c.dropIndex(tx, idx.IndexName)
		if err != nil {
			return err
		}
	}

	err = c.SchemaTable.deleteTable(tx, tableName)
	if err != nil {
		return err
	}

	return tx.Tx.DropStore(ti.StoreName)
}

// CreateIndex creates an index with the given name.
// If it already exists, returns errs.ErrIndexAlreadyExists.
func (c *Catalog) CreateIndex(tx *Transaction, info *IndexInfo) error {
	err := c.cache.AddIndex(tx, info)
	if err != nil {
		return err
	}

	err = c.SchemaTable.insertIndex(tx, info)
	if err != nil {
		return err
	}

	idx, err := c.GetIndex(tx, info.IndexName)
	if err != nil {
		return err
	}

	tb, err := c.GetTable(tx, info.TableName)
	if err != nil {
		return err
	}

	err = c.buildIndex(tx, idx, tb)
	return err
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(tx *Transaction, indexName string) (*Index, error) {
	info, err := c.cache.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	return NewIndex(tx.Tx, info.IndexName, info), nil
}

// ListIndexes returns all indexes for a given table name. If tableName is empty
// if returns a list of all indexes.
// The returned list of indexes is sorted lexicographically.
func (c *Catalog) ListIndexes(tableName string) []string {
	if tableName == "" {
		list := c.cache.ListIndexes()
		sort.Strings(list)
		return list
	}
	idxs := c.cache.GetTableIndexes(tableName)
	list := make([]string, 0, len(idxs))
	for _, idx := range idxs {
		list = append(list, idx.IndexName)
	}

	sort.Strings(list)
	return list
}

// DropIndex deletes an index from the database.
func (c *Catalog) DropIndex(tx *Transaction, name string) error {
	_, err := c.cache.DeleteIndex(tx, name)
	if err != nil {
		return err
	}

	return c.dropIndex(tx, name)
}

func (c *Catalog) dropIndex(tx *Transaction, name string) error {
	err := c.SchemaTable.deleteIndex(tx, name)
	if err != nil {
		return err
	}

	return NewIndex(tx.Tx, name, nil).Truncate()
}

// AddFieldConstraint adds a field constraint to a table.
func (c *Catalog) AddFieldConstraint(tx *Transaction, tableName string, fc FieldConstraint) error {
	newTi, _, err := c.cache.updateTable(tx, tableName, func(clone *TableInfo) error {
		return clone.FieldConstraints.Add(&fc)
	})
	if err != nil {
		return err
	}

	return c.SchemaTable.replaceTable(tx, tableName, newTi)
}

// RenameTable renames a table.
// If it doesn't exist, it returns errs.ErrTableNotFound.
func (c *Catalog) RenameTable(tx *Transaction, oldName, newName string) error {
	newTi, newIdxs, err := c.cache.updateTable(tx, oldName, func(clone *TableInfo) error {
		clone.TableName = newName
		return nil
	})
	if err != nil {
		return err
	}

	// Insert the TableInfo keyed by the newName name.
	err = c.SchemaTable.insertTable(tx, newName, newTi)
	if err != nil {
		return err
	}

	if len(newIdxs) > 0 {
		for _, idx := range newIdxs {
			idx.TableName = newName
			err = c.SchemaTable.replaceIndex(tx, idx.IndexName, idx)
			if err != nil {
				return err
			}
		}
	}

	// Delete the old table info.
	return c.SchemaTable.deleteTable(tx, oldName)
}

// ReIndex truncates and recreates selected index from scratch.
func (c *Catalog) ReIndex(tx *Transaction, indexName string) error {
	idx, err := c.GetIndex(tx, indexName)
	if err != nil {
		return err
	}

	tb, err := c.GetTable(tx, idx.Info.TableName)
	if err != nil {
		return err
	}

	err = idx.Truncate()
	if err != nil {
		return err
	}

	return c.buildIndex(tx, idx, tb)
}

func (c *Catalog) buildIndex(tx *Transaction, idx *Index, table *Table) error {
	return table.Iterate(func(d document.Document) error {
		var err error
		values := make([]document.Value, len(idx.Info.Paths))
		for i, path := range idx.Info.Paths {
			values[i], err = path.GetValueFromDocument(d)
			if err == document.ErrFieldNotFound {
				return nil
			}
			if err != nil {
				return err
			}
		}

		err = idx.Set(values, d.(document.Keyer).RawKey())
		if err != nil {
			return stringutil.Errorf("error while building the index: %w", err)
		}

		return nil
	})
}

// ReIndexAll truncates and recreates all indexes of the database from scratch.
func (c *Catalog) ReIndexAll(tx *Transaction) error {
	indexes := c.cache.ListIndexes()

	for _, indexName := range indexes {
		err := c.ReIndex(tx, indexName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Catalog) GetSequence(name string) (*Sequence, error) {
	seq, err := c.cache.GetSequence(name)
	if err != nil {
		return nil, err
	}

	return seq, nil
}

// CreateSequence creates a sequence with the given name.
func (c *Catalog) CreateSequence(tx *Transaction, name string, info *SequenceInfo) error {
	if info == nil {
		info = new(SequenceInfo)
	}
	info.Name = name

	err := c.SchemaTable.insertSequence(tx, info)
	if err != nil {
		return err
	}

	err = c.SequenceTable.InitSequence(tx, info.Name)
	if err != nil {
		return err
	}

	return c.cache.AddSequence(tx, info)
}

// DropSequence deletes a sequence from the catalog.
func (c *Catalog) DropSequence(tx *Transaction, name string) error {
	_, err := c.cache.DeleteSequence(tx, name)
	if err != nil {
		return err
	}

	return c.SchemaTable.deleteSequence(tx, name)
}

type catalogCache struct {
	tables           map[string]*TableInfo
	indexes          map[string]*IndexInfo
	indexesPerTables map[string][]*IndexInfo
	sequences        map[string]*Sequence

	mu sync.RWMutex
}

func newCatalogCache() *catalogCache {
	return &catalogCache{
		tables:           make(map[string]*TableInfo),
		indexes:          make(map[string]*IndexInfo),
		indexesPerTables: make(map[string][]*IndexInfo),
		sequences:        make(map[string]*Sequence),
	}
}

func (c *catalogCache) load(tables []TableInfo, indexes []IndexInfo, sequences []Sequence) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range tables {
		c.tables[tables[i].TableName] = &tables[i]
	}

	for i := range indexes {
		c.indexes[indexes[i].IndexName] = &indexes[i]
		c.indexesPerTables[indexes[i].TableName] = append(c.indexesPerTables[indexes[i].TableName], &indexes[i])
	}

	for i := range sequences {
		c.sequences[sequences[i].Info.Name] = &sequences[i]
	}
}

func (c *catalogCache) clone() *catalogCache {
	clone := newCatalogCache()

	for k, v := range c.tables {
		clone.tables[k] = v
	}
	for k, v := range c.indexes {
		clone.indexes[k] = v
	}
	for k, v := range c.indexesPerTables {
		clone.indexesPerTables[k] = v
	}
	for k, v := range c.sequences {
		clone.sequences[k] = v
	}

	return clone
}

func (c *catalogCache) AddTable(tx *Transaction, info *TableInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// checking if table exists
	if _, ok := c.tables[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	// checking if sequence exists with the same name
	if _, ok := c.sequences[info.TableName]; ok {
		return errs.AlreadyExistsError{Name: info.TableName}
	}

	c.tables[info.TableName] = info

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, info.TableName)
	})

	return nil
}

func (c *catalogCache) DeleteTable(tx *Transaction, tableName string) (*TableInfo, []*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, errs.ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	delete(c.tables, tableName)
	delete(c.indexesPerTables, tableName)
	var removedIndexes []*IndexInfo

	for _, idx := range c.indexes {
		if idx.TableName != tableName {
			continue
		}

		removedIndexes = append(removedIndexes, idx)
	}

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.tables[tableName] = ti

		for _, idx := range removedIndexes {
			c.indexes[idx.IndexName] = idx
		}

		if len(removedIndexes) > 0 {
			c.indexesPerTables[tableName] = removedIndexes
		}
	})

	return ti, removedIndexes, nil
}

func (c *catalogCache) GetTable(tableName string) (*TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, errs.ErrTableNotFound
	}

	return ti, nil
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

func (c *catalogCache) AddIndex(tx *Transaction, info *IndexInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// auto-generate index name if needed
	if info.IndexName == "" {
		info.IndexName = stringutil.Sprintf("%s_%s_idx", info.TableName, pathsToIndexName(info.Paths))
		if _, ok := c.indexes[info.IndexName]; ok {
			i := 1
			for {
				info.IndexName = stringutil.Sprintf("%s_%s_idx%d", info.TableName, pathsToIndexName(info.Paths), i)
				if _, ok := c.indexes[info.IndexName]; !ok {
					break
				}

				i++
			}
		}
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// checking if table exists with the same name
	if _, ok := c.tables[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// checking if sequence exists with the same name
	if _, ok := c.sequences[info.IndexName]; ok {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	// get the associated table
	ti, ok := c.tables[info.TableName]
	if !ok {
		return errs.ErrTableNotFound
	}

	// if the index is created on a field on which we know the type then create a typed index.
	// if the given info contained existing types, they are overriden.
	info.Types = nil

OUTER:
	for _, path := range info.Paths {
		for _, fc := range ti.FieldConstraints {
			if fc.Path.IsEqual(path) {
				// a constraint may or may not enforce a type
				if fc.Type != 0 {
					info.Types = append(info.Types, document.ValueType(fc.Type))
				}

				continue OUTER
			}
		}

		// no type was inferred for that path, add it to the index as untyped
		info.Types = append(info.Types, document.ValueType(0))
	}

	c.indexes[info.IndexName] = info
	previousIndexes := c.indexesPerTables[info.TableName]
	c.indexesPerTables[info.TableName] = append(c.indexesPerTables[info.TableName], info)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.indexes, info.IndexName)

		if len(previousIndexes) == 0 {
			delete(c.indexesPerTables, info.TableName)
		} else {
			c.indexesPerTables[info.TableName] = previousIndexes
		}
	})

	return nil
}

func (c *catalogCache) DeleteIndex(tx *Transaction, indexName string) (*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the index exists
	info, ok := c.indexes[indexName]
	if !ok {
		return nil, errs.ErrIndexNotFound
	}

	// check if the index has been created by a table constraint
	if info.ConstraintPath != nil {
		return nil, stringutil.Errorf("cannot drop index %s because constraint on %s(%s) requires it", info.IndexName, info.TableName, info.ConstraintPath)
	}

	// remove it from the global map of indexes
	delete(c.indexes, indexName)

	// build a new list of indexes for the related table.
	// the previous list must not be modified.
	newIndexlist := make([]*IndexInfo, 0, len(c.indexesPerTables[info.TableName]))
	for _, idx := range c.indexesPerTables[info.TableName] {
		if idx.IndexName != indexName {
			newIndexlist = append(newIndexlist, idx)
		}
	}

	oldIndexList := c.indexesPerTables[info.TableName]
	c.indexesPerTables[info.TableName] = newIndexlist

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.indexes[indexName] = info
		c.indexesPerTables[info.TableName] = oldIndexList
	})

	return info, nil
}

func (c *catalogCache) GetIndex(indexName string) (*IndexInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.indexes[indexName]
	if !ok {
		return nil, errs.ErrIndexNotFound
	}

	return info, nil
}

func (c *catalogCache) ListIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	indexes := make([]string, 0, len(c.indexes))
	for name := range c.indexes {
		indexes = append(indexes, name)
	}

	return indexes
}

func (c *catalogCache) GetTableIndexes(tableName string) []*IndexInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.indexesPerTables[tableName]
}

func (c *catalogCache) updateTable(tx *Transaction, tableName string, fn func(clone *TableInfo) error) (*TableInfo, []*IndexInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ti, ok := c.tables[tableName]
	if !ok {
		return nil, nil, errs.ErrTableNotFound
	}

	if ti.ReadOnly {
		return nil, nil, errors.New("cannot write to read-only table")
	}

	clone := ti.Clone()
	err := fn(clone)
	if err != nil {
		return nil, nil, err
	}

	var oldIndexes, newIndexes []*IndexInfo
	// if the table has been renamed, we need to modify indexes
	// to point to the new table
	if clone.TableName != tableName {
		// delete the old table name from the table list
		delete(c.tables, tableName)

		for _, idx := range c.indexesPerTables[tableName] {
			idxClone := idx.Clone()
			idxClone.TableName = clone.TableName
			newIndexes = append(newIndexes, idxClone)
			oldIndexes = append(oldIndexes, idx)
			c.indexes[idxClone.IndexName] = idxClone
		}
		if len(newIndexes) > 0 {
			c.indexesPerTables[clone.TableName] = newIndexes
			delete(c.indexesPerTables, tableName)
		}
	}

	c.tables[clone.TableName] = clone

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.tables, clone.TableName)
		c.tables[tableName] = ti

		for _, idx := range oldIndexes {
			c.indexes[idx.IndexName] = idx
		}

		if clone.TableName != tableName {
			delete(c.indexesPerTables, clone.TableName)
			c.indexesPerTables[tableName] = oldIndexes
		}
	})

	return clone, newIndexes, nil
}

func (c *catalogCache) AddSequence(tx *Transaction, info *SequenceInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// checking if sequence exists with the same name
	if _, ok := c.sequences[info.Name]; ok {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	// checking if table exists with the same name
	if _, ok := c.tables[info.Name]; ok {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	// checking if index exists with the same name
	if _, ok := c.indexes[info.Name]; ok {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	c.sequences[info.Name] = &Sequence{Info: info}

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.sequences, info.Name)
	})

	return nil
}

func (c *catalogCache) GetSequence(name string) (*Sequence, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	seq, ok := c.sequences[name]
	if !ok {
		return nil, errs.ErrSequenceNotFound
	}

	return seq, nil
}

func (c *catalogCache) DeleteSequence(tx *Transaction, name string) (*Sequence, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if the sequence exists
	seq, ok := c.sequences[name]
	if !ok {
		return nil, errs.ErrSequenceNotFound
	}

	// remove it from the global map of sequences
	delete(c.sequences, name)

	tx.OnRollbackHooks = append(tx.OnRollbackHooks, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.sequences[name] = seq
	})

	return seq, nil
}

type CatalogTable struct {
	info *TableInfo
}

func NewCatalogTable(tx *Transaction) *CatalogTable {
	return &CatalogTable{
		info: &TableInfo{
			TableName: CatalogTableName,
			StoreName: []byte(CatalogTableName),
			FieldConstraints: []*FieldConstraint{
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

func (s *CatalogTable) Init(tx *Transaction) error {
	_, err := tx.Tx.GetStore([]byte(CatalogTableName))
	if err == engine.ErrStoreNotFound {
		err = tx.Tx.CreateStore([]byte(CatalogTableName))
	}
	return err
}

func (s *CatalogTable) GetTable(tx *Transaction) *Table {
	st, err := tx.Tx.GetStore([]byte(CatalogTableName))
	if err != nil {
		panic(stringutil.Sprintf("database incorrectly setup: missing %q table: %v", CatalogTableName, err))
	}

	return &Table{
		Tx:    tx,
		Store: st,
		Info:  s.info,
	}
}

func (s *CatalogTable) tableInfoToDocument(ti *TableInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(ti.TableName))
	buf.Add("type", document.NewTextValue(CatalogTableTableType))
	buf.Add("store_name", document.NewBlobValue(ti.StoreName))
	buf.Add("sql", document.NewTextValue(ti.String()))
	return buf
}

func (s *CatalogTable) indexInfoToDocument(i *IndexInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(i.IndexName))
	buf.Add("type", document.NewTextValue(CatalogTableIndexType))
	buf.Add("store_name", document.NewBlobValue(i.StoreName))
	buf.Add("table_name", document.NewTextValue(i.TableName))
	buf.Add("sql", document.NewTextValue(i.String()))
	if i.ConstraintPath != nil {
		buf.Add("constraint_path", document.NewTextValue(i.ConstraintPath.String()))
	}

	return buf
}

func (s *CatalogTable) sequenceInfoToDocument(seq *SequenceInfo) document.Document {
	buf := document.NewFieldBuffer()
	buf.Add("name", document.NewTextValue(seq.Name))
	buf.Add("type", document.NewTextValue(CatalogTableSequenceType))
	buf.Add("sql", document.NewTextValue(seq.String()))

	return buf
}

// insertTable inserts a new tableInfo for the given table name.
// If info.StoreName is nil, it generates one and stores it in info.
func (s *CatalogTable) insertTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := s.GetTable(tx)

	if info.StoreName == nil {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, seq)
		info.StoreName = buf[:n]
	}

	_, err := tb.Insert(s.tableInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: tableName}
	}

	return err
}

// Replace replaces tableName table information with the new info.
func (s *CatalogTable) replaceTable(tx *Transaction, tableName string, info *TableInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(tableName), s.tableInfoToDocument(info))
	return err
}

func (s *CatalogTable) deleteTable(tx *Transaction, tableName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(tableName))
}

func (s *CatalogTable) insertIndex(tx *Transaction, info *IndexInfo) error {
	tb := s.GetTable(tx)

	if info.StoreName == nil {
		seq, err := tb.Store.NextSequence()
		if err != nil {
			return err
		}

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, seq)
		info.StoreName = buf[:n]
	}

	_, err := tb.Insert(s.indexInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.IndexName}
	}

	return err
}

func (s *CatalogTable) replaceIndex(tx *Transaction, indexName string, info *IndexInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(indexName), s.indexInfoToDocument(info))
	return err
}

func (s *CatalogTable) deleteIndex(tx *Transaction, indexName string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(indexName))
}

func (s *CatalogTable) insertSequence(tx *Transaction, info *SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Insert(s.sequenceInfoToDocument(info))
	if err == errs.ErrDuplicateDocument {
		return errs.AlreadyExistsError{Name: info.Name}
	}

	return err
}

func (s *CatalogTable) replaceSequence(tx *Transaction, name string, info *SequenceInfo) error {
	tb := s.GetTable(tx)

	_, err := tb.Replace([]byte(name), s.sequenceInfoToDocument(info))
	return err
}

func (s *CatalogTable) deleteSequence(tx *Transaction, name string) error {
	tb := s.GetTable(tx)

	return tb.Delete([]byte(name))
}
