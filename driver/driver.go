package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
)

func init() {
	sql.Register("genji", sqlDriver{})
}

var (
	_ driver.Driver        = (*sqlDriver)(nil)
	_ driver.DriverContext = (*sqlDriver)(nil)
)

// sqlDriver is a driver.Driver that can open a new connection to a Genji database.
// It is the driver used to register Genji against the database/sql package.
type sqlDriver struct{}

func (d sqlDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("requires go1.10 or greater")
}

func (d sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	db, err := genji.Open(name)
	if err != nil {
		return nil, err
	}

	c := &connector{
		db:     db,
		driver: d,
	}
	runtime.SetFinalizer(c, (*connector).Close)

	return c, nil
}

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

type connector struct {
	driver driver.Driver

	db *genji.DB

	closeOnce sync.Once
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &conn{db: c.db}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

func (c *connector) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.db.Close()
	})
	return err
}

// conn represents a connection to the Genji database.
// It implements the database/sql/driver.Conn interface.
type conn struct {
	db *genji.DB
	tx *genji.Tx
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	var s *genji.Statement
	var err error

	if c.tx != nil {
		s, err = c.tx.Prepare(q)
	} else {
		s, err = c.db.Prepare(q)
	}
	if err != nil {
		return nil, err
	}

	return stmt{
		stmt: s,
	}, nil
}

// Close closes any ongoing transaction.
func (c *conn) Close() error {
	if c.tx != nil {
		return c.tx.Rollback()
	}

	return nil
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// It uses the ReadOnly option to determine whether to start a read-only or read/write transaction.
// If the Isolation option is non zero, an error is returned.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 {
		return nil, errors.New("isolation levels are not supported")
	}

	db := c.db.WithContext(ctx)

	// if the ReadOnly flag is explicitly specified, create a read-only transaction,
	// otherwise create a read/write transaction.
	var err error
	c.tx, err = db.Begin(!opts.ReadOnly)

	return c, err
}

func (c *conn) Commit() error {
	err := c.tx.Commit()
	c.tx = nil
	return err
}

func (c *conn) Rollback() error {
	err := c.tx.Rollback()
	c.tx = nil
	return err
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type stmt struct {
	stmt *genji.Statement
}

// NumInput returns the number of placeholder parameters.
func (s stmt) NumInput() int { return -1 }

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

// CheckNamedValue has the same behaviour as driver.DefaultParameterConverter, except that
// it allows document.Document to be passed as parameters.
// It implements the driver.NamedValueChecker interface.
func (s stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(document.Document); ok {
		return nil
	}

	if _, ok := nv.Value.(document.Scanner); ok {
		return nil
	}

	var err error
	val, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
	if err == nil {
		nv.Value = val
		return nil
	}

	return nil
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return result{}, s.stmt.Exec(driverNamedValueToParams(args)...)
}

type result struct{}

// LastInsertId is not supported and returns an error.
func (r result) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

// RowsAffected is not supported and returns an error.
func (r result) RowsAffected() (int64, error) {
	return 0, errors.New("not supported")
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	res, err := s.stmt.Query(driverNamedValueToParams(args)...)
	if err != nil {
		return nil, err
	}

	rs := newRecordStream(res)
	rs.fields = res.Fields()
	return rs, nil
}

func driverNamedValueToParams(args []driver.NamedValue) []interface{} {
	params := make([]interface{}, len(args))
	for i, arg := range args {
		var p environment.Param
		p.Name = arg.Name
		p.Value = arg.Value
		params[i] = p
	}

	return params
}

// Close does nothing.
func (s stmt) Close() error {
	return nil
}

var errStop = errors.New("stop")

type documentStream struct {
	res      *genji.Result
	cancelFn func()
	c        chan doc
	wg       sync.WaitGroup
	fields   []string
}

type doc struct {
	d   document.Document
	err error
}

func newRecordStream(res *genji.Result) *documentStream {
	ctx, cancel := context.WithCancel(context.Background())

	ds := documentStream{
		res:      res,
		cancelFn: cancel,
		c:        make(chan doc),
	}
	ds.wg.Add(1)

	go ds.iterate(ctx)

	return &ds
}

func (rs *documentStream) iterate(ctx context.Context) {
	defer rs.wg.Done()
	defer close(rs.c)

	select {
	case <-ctx.Done():
		return
	case <-rs.c:
	}

	err := rs.res.Iterate(func(d document.Document) error {
		select {
		case <-ctx.Done():
			return errStop
		case rs.c <- doc{
			d: d,
		}:

			select {
			case <-ctx.Done():
				return errStop
			case <-rs.c:
				return nil
			}
		}
	})

	if err == errStop || err == nil {
		return
	}
	if err != nil {
		rs.c <- doc{
			err: err,
		}
		return
	}
}

// Columns returns the fields selected by the SELECT statement.
func (rs *documentStream) Columns() []string {
	return rs.res.Fields()
}

// Close closes the rows iterator.
func (rs *documentStream) Close() error {
	rs.cancelFn()
	return rs.res.Close()
}

func (rs *documentStream) Next(dest []driver.Value) error {
	rs.c <- doc{}

	doc, ok := <-rs.c
	if !ok {
		return io.EOF
	}

	if doc.err != nil {
		return doc.err
	}

	for i := range rs.fields {
		if rs.fields[i] == "*" {
			dest[i] = doc.d

			continue
		}

		f, err := doc.d.GetByField(rs.fields[i])
		if err != nil {
			return err
		}

		dest[i] = f.V()
	}

	return nil
}

type valueScanner struct {
	dest interface{}
}

func (v valueScanner) Scan(src interface{}) error {
	switch t := src.(type) {
	case document.Document:
		return document.StructScan(t, v.dest)
	case document.Array:
		return document.SliceScan(t, v.dest)
	case document.Value:
		return document.ScanValue(t, src)
	}

	vv, err := document.NewValue(src)
	if err != nil {
		return err
	}

	return document.ScanValue(vv, v.dest)
}

// Scanner turns a variable into a sql.Scanner.
// x must be a pointer to a valid variable.
func Scanner(x interface{}) sql.Scanner {
	return valueScanner{x}
}
