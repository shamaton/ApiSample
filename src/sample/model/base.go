package model

import (
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"reflect"
	"sample/DBI"
	"strings"
)

// base
//////////////////////////////
type Base interface {
	Find(*gin.Context, interface{}, builder.SelectBuilder) error
}

type base struct {
	table string
}

func (b *base) Find(c *gin.Context, holder interface{}, sb builder.SelectBuilder) error {

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)
	log.Info(dbTableConf)

	// holderからカラム情報を取得
	var columns []string
	var pkKeys []string
	var pkValues []interface{}
	var shardKey interface{}
	val := reflect.ValueOf(holder).Elem()
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		// カラム
		column := strings.ToLower(typeField.Name)
		columns = append(columns, column)

		// プライマリキー
		if tag.Get("base") == "pk" {
			pkKeys = append(pkKeys, column)
			pkValues = append(pkValues, valueField.Interface())
		}

		// shard keyを取得
		if dbTableConf.IsUseTypeShard() && tag.Get("shard") == "true" {
			// TODO:2度設定はダメ
			shardKey = valueField.Interface()
			log.Debug("shardkey : ", typeField.Name, " : ", shardKey)
		}
	}
	columnsStr := strings.Join(columns, ",")
	log.Debug(columnsStr)
	log.Debug("pks ", pkKeys, " values ", pkValues)

	// shardの場合、shard_idを取得
	// TODO:shard key check
	if dbTableConf.IsUseTypeShard() {
		repo := NewShardRepo()
		shardInfo, err := repo.findShardId(c, dbTableConf.ShardType, shardKey)
		if err != nil {
			return err
		}

		log.Debug("shard info : ", shardInfo)

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

/*

func (b *base) Find(c *gin.Context, holder interface{}, sb builder.SelectBuilder) error {
	val := reflect.ValueOf(holder).Elem()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		tag := typeField.Tag

		log.Infof("Field Name: %s,\t Field Value: %v,\t Tag Value: %s\n", typeField.Name, valueField.Interface(), tag.Get("db"))
	}
	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)
	log.Info(dbTableConf)

	// shardの場合、shard_idを取得

	sql, args, _ := sb.ToSql()
	dbMap, err := DBI.GetDBConnection(c, "table_name")
	if err != nil {
		log.Error("db error!!")
		return err
	}

	err = dbMap.SelectOne(holder, sql, args...)
	return err
}
*/
