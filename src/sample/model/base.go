package model

import (
	"errors"
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"reflect"
	db "sample/DBI"
	"strings"
)

// base
//////////////////////////////
type Base interface {
	Find(*gin.Context, interface{}, ...interface{}) error

	Update(map[string]interface{})
}

type base struct {
	table string //<! テーブル名
}

/**************************************************************************************************/
/*!
 *  pkを利用したfetchを行う
 *
 *  \param   c       : コンテキスト
 *  \param   holder  : テーブルデータ構造体
 *  \param   options : モード[W,R,BAK] ロック[FOR_UPDATE]
 *  \return  エラー（正常時はholderにデータを取得する）
 */
/**************************************************************************************************/
func (b *base) Find(c *gin.Context, holder interface{}, options ...interface{}) error {

	// optionsの解析
	mode, isForUpdate, _, _, err := b.optionCheck(options...)
	if err != nil {
		log.Error("invalid options set!!")
		return err
	}
	log.Debug("mode : ", mode, " | for_update : ", isForUpdate)

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)

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
		}
	}

	// pkMapをチェックしておく
	if len(pkMap) < 1 {
		err = errors.New("must be set pks in struct!!")
		log.Error(err)
		return err
	}

	// shardの場合、shard_idを取得
	var shardId int
	if dbTableConf.IsUseTypeShard() {
		// value check
		if shardKey == nil {
			return errors.New("not set shard_key!!")
		}
		// 検索
		repo := NewShardRepo()
		shardId, err = repo.findShardId(c, dbTableConf.ShardType, shardKey)
		if err != nil {
			return err
		}
	}

	// SQL生成
	var sb builder.SelectBuilder
	columnStr := strings.Join(columns, ",")
	sb = builder.Select(columnStr).From(b.table).Where(pkMap)
	if isForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}
	sql, args, err := sb.ToSql()

	// とりあえず分けてみる
	if isForUpdate {
		tx, err := db.GetTransaction(c, dbTableConf.IsUseTypeShard(), shardId)
		if err != nil {
			log.Error("transaction error!!")
			return err
		}
		err = tx.SelectOne(holder, sql, args...)
	} else {
		dbMap, err := db.GetDBConnection(c, mode, dbTableConf.IsUseTypeShard(), shardId)
		if err != nil {
			log.Error("db connection error!!")
			return err
		}
		// fetch
		err = dbMap.SelectOne(holder, sql, args...)
	}

	// TODO:デバッグでは通常selectで複数行取得されないことも確認する
	return err
}

func (b *base) Finds(c *gin.Context, holders interface{}, options ...interface{}) error {

	mode, isForUpdate, shardKey, shardId, err := b.optionCheck(options...)
	if err != nil {
		log.Error("invalid options set!!")
		return err
	}
	log.Debug("mode : ", mode, " | for_update : ", isForUpdate, shardKey, shardId)

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)
	//log.Debug(dbTableConf)

	// holder(table struct)からカラム情報を取得
	var columns []string

	// pkはwhere条件に必ず使う
	var pkMap = builder.Eq{}

	val := reflect.ValueOf(holders).Elem()
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
			//log.Debug("shardkey : ", typeField.Name, " : ", shardKey)
		}
	}

	// pkMapをチェックしておく
	if len(pkMap) < 1 {
		err = errors.New("must be set pks in struct!!")
		log.Error(err)
		return err
	}

	// shardの場合、shard_idを取得
	if dbTableConf.IsUseTypeShard() {
		// value check
		if shardKey == nil {
			return errors.New("not set shard_key!!")
		}
		// 検索
		repo := NewShardRepo()
		shardId, err = repo.findShardId(c, dbTableConf.ShardType, shardKey)
		if err != nil {
			return err
		}

		//log.Debug("shard info : ", shardId)

	}

	// SQL生成
	var sb builder.SelectBuilder
	columnStr := strings.Join(columns, ",")
	sb = builder.Select(columnStr).From(b.table).Where(pkMap)
	if isForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}
	sql, args, err := sb.ToSql()

	// とりあえず分けてみる
	if isForUpdate {
		tx, err := db.GetTransaction(c, dbTableConf.IsUseTypeShard(), shardId)
		if err != nil {
			log.Error("transaction error!!")
			return err
		}
		// select
		_, err = tx.Select(holders, sql, args...)
	} else {
		dbMap, err := db.GetDBConnection(c, mode, dbTableConf.IsUseTypeShard(), shardId)
		if err != nil {
			log.Error("db connection error!!")
			return err
		}
		// select
		_, err = dbMap.Select(holders, sql, args...)
	}

	// TODO:デバッグでは通常selectで複数行取得されないことも確認する
	return err
}

func (b *base) Update(hoge map[string]interface{}) {
	log.Debug(hoge)
	log.Debug("aaaaaa")
}

func Create() {

}

func Delete() {

}

/*
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
*/

/**************************************************************************************************/
/*!
 *  Find,Create,Update,Delete経由のオプションを処理する
 *
 *  optionMapには mode, for_update, shardKey, shardIdが設定できる
 *  ここは整理するかもしれない
 *  \param   options : モード[W,R,BAK] ロック[FOR_UPDATE]
 *  \return  モード、ロックするか、エラー
 */
/**************************************************************************************************/
func (b *base) optionCheck(options ...interface{}) (string, bool, interface{}, int, error) {
	var err error

	var mode = db.MODE_R
	var isForUpdate = false
	var shardKey interface{}
	var shardId int

	var optionMap map[string]interface{}

	for _, v := range options {

		switch v.(type) {
		case string:
			str := v.(string)
			if str == db.MODE_W || str == db.MODE_R || str == db.MODE_BAK {
				mode = str
			} else if str == db.FOR_UPDATE {
				isForUpdate = true
			} else {
				err = errors.New("unknown option!!")
				break
			}

		case map[string]interface{}:
			// 後で処理する
			optionMap = v.(map[string]interface{})

		default:
			err = errors.New("can not check this type!!")
			log.Error(v)
			break
		}
	}

	// optionMapの解析
	// TODO:専用のtypeを作成する
	for k, v := range optionMap {

		switch k {
		case "mode":
			str := v.(string)
			if str == db.MODE_W || str == db.MODE_R || str == db.MODE_BAK {
				mode = str
			} else {
				err = errors.New("invalid mode!!")
				break
			}

		case "for_update":
			isForUpdate = true

		case "shard_key":
			shardKey = v

		case "shard_id":
			value, isInt := v.(int)
			// 型チェック & 範囲チェック
			if !isInt {
				err = errors.New("type not integer!!")
				break
			} else if value < 1 || value > 2 {
				// TODO:ちゃんとチェックする
				err = errors.New("over shard id range!!")
				break
			}
			shardId = v.(int)

		default:
			err = errors.New("invalid key!!")

		}
	}

	// shard系は1つのみを想定する
	if shardKey != nil && shardId > 0 {
		err = errors.New("can't set shardKey and shardId in optionMap!!")
		return mode, isForUpdate, shardKey, shardId, err
	}

	// for updateな場合、MODEは必ずW
	if isForUpdate {
		mode = db.MODE_W
	}
	return mode, isForUpdate, shardKey, shardId, err
}
