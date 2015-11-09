package model

import (
	"errors"
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

func (b *base) Find(c *gin.Context, holder interface{}) error {

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)
	log.Info(dbTableConf)

	// holder(table struct)からカラム情報を取得
	var columns []string
	var shardKey interface{}

	// pkはwhere条件に必ず使う
	var pkMap = builder.Eq{}

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
			pkMap[column] = valueField.Interface()
		}

		// shard keyを取得
		if dbTableConf.IsUseTypeShard() && tag.Get("shard") == "true" {
			// 2度設定はダメ
			if shardKey != nil {
				return errors.New("multiple shard key not available!!")
			}
			shardKey = valueField.Interface()
			log.Debug("shardkey : ", typeField.Name, " : ", shardKey)
		}
	}

	// shardの場合、shard_idを取得
	if dbTableConf.IsUseTypeShard() {
		// value check
		if shardKey == nil {
			return errors.New("not set shard_key!!")
		}
		// 検索
		repo := NewShardRepo()
		shardId, err := repo.findShardId(c, dbTableConf.ShardType, shardKey)
		if err != nil {
			return err
		}

		log.Debug("shard info : ", shardId)

	}

	// SQL生成
	columnStr := strings.Join(columns, ",")

	sql, args, err := builder.Select(columnStr).From(b.table).Where(pkMap).ToSql()

	dbMap, err := DBI.GetDBConnection(c, "table_name")
	if err != nil {
		log.Error("db error!!")
		return err
	}

	err = dbMap.SelectOne(holder, sql, args...)
	return err
}

func (b *base) FindBySelectBuilder(c *gin.Context, holder interface{}, sb builder.SelectBuilder, isForUpdate bool) error {

	sql, args, _ := sb.ToSql()
	dbMap, err := DBI.GetDBConnection(c, "table_name")
	if err != nil {
		log.Error("db error!!")
		return err
	}

	err = dbMap.SelectOne(holder, sql, args...)
	return err
}
