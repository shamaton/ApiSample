package model

import (
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"reflect"
	"sample/DBI"
)

// base
//////////////////////////////
type Base interface {
	Find(*gin.Context, interface{}, builder.SelectBuilder) error
}

type base struct {
}

func (b *base) Find(c *gin.Context, holder interface{}, sb builder.SelectBuilder) error {
	val := reflect.ValueOf(holder).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		log.Infof("Field Name: %s,\t Field Value: %v,\t Tag Value: %s\n", typeField.Name, valueField.Interface(), tag.Get("tag_name"))
	}

	sql, args, _ := sb.ToSql()
	dbMap, err := DBI.GetDBConnection(c, "table_name")
	if err != nil {
		log.Error("db error!!")
		return err
	}

	err = dbMap.SelectOne(holder, sql, args...)
	return err
}
