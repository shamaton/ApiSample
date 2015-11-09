package model

import (
	"sample/shamoto/core"

	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"reflect"
)

// base
//////////////////////////////
type Base interface {
	Find(interface{}, builder.SelectBuilder) error
}

type base struct {
}

func (b *base) Find(holder interface{}, sb builder.SelectBuilder) error {
	val := reflect.ValueOf(holder).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		log.Infof("Field Name: %s,\t Field Value: %v,\t Tag Value: %s\n", typeField.Name, valueField.Interface(), tag.Get("tag_name"))
	}

	sql, args, _ := sb.ToSql()
	dbMap := core.GetDB()
	err := dbMap.SelectOne(holder, sql, args...)
	return err
}
