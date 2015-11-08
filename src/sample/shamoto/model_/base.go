package model_

import (
	"sample/shamoto/core"

	builder "github.com/Masterminds/squirrel"
)


// base
//////////////////////////////
type Base interface {
	Find(interface{}, builder.SelectBuilder) error
}

type base struct {

}

func (b *base) Find(holder interface{}, sb builder.SelectBuilder) error {
	sql, args, _ := sb.ToSql()
	dbMap := core.GetDB()
	err := dbMap.SelectOne(holder, sql, args...)
	return err
}


