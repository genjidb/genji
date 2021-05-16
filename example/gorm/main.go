package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/memoryengine"
	gdriver "github.com/genjidb/genji/sql/driver"
	"gorm.io/gorm"
)

func run() error {
	ctx := context.Background()

	store := memoryengine.NewEngine()
	gjdb, err := genji.New(ctx, store)
	if err != nil {
		return err
	}
	/*
		driver, ok := gdriver.NewDriver(gjdb).(driver.DriverContext)
		if !ok {
			return gorm.ErrNotImplemented
		}
		conn, err := driver.OpenConnector("")
		if err != nil {
			return err
		}
	*/
	conn := gdriver.NewConnector(gjdb)
	sqlDB := sql.OpenDB(conn)
	dialector := NewDialector(sqlDB)
	conf := &gorm.Config{Dialector: dialector, ConnPool: sqlDB}
	db, err := gorm.Open(dialector, conf)
	if err != nil {
		return err
	}
	if err := db.AutoMigrate(&Entry{}); err != nil {
		return err
	}
	db.Create(&Entry{Value: 4, ID: 1})
	db.Create(&Entry{Value: 10, ID: 2})
	db.Create(&Entry{Value: 30, ID: 3})

	var e Entry
	out := db.Where("value = ?", 30).Find(&e)
	if out.Error != nil {
		return out.Error
	}
	if e.Value != 30 {
		return errors.New("value was incorrect")
	}
	return nil
}

// Entry is an entry in the database.
type Entry struct {
	ID    int `gorm:"primaryKey"`
	Value int `json:"value"`
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
