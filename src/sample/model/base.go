package model

import (
	"errors"
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"reflect"
	db "sample/DBI"
	"strconv"
	"strings"
)

// 一旦ここに
type Option map[string]interface{}
type Condition map[string]interface{}
type WhereCondition [][]interface{}
type OrderByCondition [][]string
type In []interface{}

// base
//////////////////////////////
type Base interface {
	Find(*gin.Context, interface{}, ...interface{}) error
	Finds(*gin.Context, interface{}, Condition, ...interface{}) error

	Update(*gin.Context, interface{}, ...interface{}) error
}

type base struct {
	table string //<! テーブル名
}

/**
 *  Find method
 */
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

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)

	// holderから各要素を取得
	columns, _, pkMap, shardKey, err := b.getTableInfoFromStructData(holder, dbTableConf)
	if err != nil {
		log.Error("read error in struct data")
		return err
	}

	// shardの場合、shard_idを取得
	shardId, err := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if err != nil {
		return err
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

/**
 *  Finds method
 */
/**************************************************************************************************/
/*!
 *  指定テーブルへのselectを行う
 *
 *  \param   c         : コンテキスト
 *  \param   holders   : select結果格納先
 *  \param   condition : where, orderに利用する条件
 *  \param   options   : モード[W,R,BAK] ロック[FOR_UPDATE]
 *  \return  エラー（正常時はholdersにデータを取得する）
 */
/**************************************************************************************************/
func (b *base) Finds(c *gin.Context, holders interface{}, condition map[string]interface{}, options ...interface{}) error {

	wSql, wArgs, orders, err := b.conditionCheck(condition)
	if err != nil {
		log.Error("invalid condition set!!")
		return err
	}

	mode, isForUpdate, shardKey, shardId, err := b.optionCheck(options...)
	if err != nil {
		log.Error("invalid options set!!")
		return err
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)

	// 構造体のポインタ配列(holder)からカラムを取得する
	// holdersは配列のポインタであること
	var structRef reflect.Type
	hRef := reflect.TypeOf(holders)
	if hRef.Kind() != reflect.Ptr {
		return errors.New("holders type is not Ptr!!")
	}
	// 次にスライスであること
	sRef := hRef.Elem()
	if sRef.Kind() != reflect.Slice {
		return errors.New("holders element type is not Slice!!")
	}
	// 最後に構造体であること
	structRef = sRef.Elem()
	if structRef.Kind() != reflect.Struct {
		return errors.New("holders slice element type is not Struct!!")
	}

	// カラムの取得
	var columns []string
	for i := 0; i < structRef.NumField(); i++ {
		field := structRef.Field(i)

		// カラム
		column := strings.ToLower(field.Name)
		columns = append(columns, column)
	}

	// shardIdをoptionで受け取ってないなら、shardKeyから取得する
	if dbTableConf.IsUseTypeShard() && shardId == 0 {
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

	sb = builder.Select(columnStr).From(b.table)
	if len(wSql) > 0 && len(wArgs) > 0 {
		sb = sb.Where(wSql, wArgs...)
	}
	if len(orders) > 0 {
		sb = sb.OrderBy(orders...)
	}

	// FOR UPDATEは一旦封印しておく
	/*
		if isForUpdate {
			sb = sb.Suffix("FOR UPDATE")
		}
	*/
	sql, args, err := sb.ToSql()
	log.Debug(sql)

	// とりあえず分けてみる
	// TODO:ここのforupdateどうするか
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

	return err
}

/**
 *  Update method
 */
/**************************************************************************************************/
/*!
 *  PRIMARY KEYを用いたUPDATEを実行する
 *
 *  prevHolder(更新前データ)が存在する場合、比較して値を更新するべきものだけSETする
 *  そうでない場合、PK以外の値全てをSETするので注意
 *
 *  \param   condition : where, orderに利用する条件
 *  \return  where文, where引数、orderBy用配列、エラー
 */
/**************************************************************************************************/
func (b *base) Update(c *gin.Context, holder interface{}, prevHolder ...interface{}) error {
	//return 0, nil

	var err error

	// 更新前のデータがある場合比較する
	// データの更新はないけど、データ更新がなかったという更新(update_at)のみしたい場合は...?
	/*
		if prevHolder == holder {
			// 更新の必要なし
			return err
		}
	*/
	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, err := dbTableConfRepo.Find(c, b.table)

	// holderから各要素を取得
	_, valueMap, pkMap, shardKey, err := b.getTableInfoFromStructData(holder, dbTableConf)
	if err != nil {
		log.Error("read error in struct data")
		return err
	}

	// TODO: 更新前のデータがある場合、更新すべき値を抽出する

	// shardの場合、shard_idを取得
	shardId, err := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if err != nil {
		return err
	}

	// SQL生成
	sql, args, err := builder.Update(b.table).SetMap(valueMap).Where(pkMap).ToSql()
	if err != nil {
		log.Error("sql maker error!!")
		return err
	}
	// tx
	tx, err := db.GetTransaction(c, dbTableConf.IsUseTypeShard(), shardId)
	if err != nil {
		log.Error("transaction error!!")
		return err
	}
	// UPDATE(tx.updateはpkに対してまでsetするので使わない)
	_, err = tx.Exec(sql, args...)
	return err

}

/**
 *  Create method
 */
func Create() {

}

/**
 *  Delete method
 */
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
 *  データ構造体からDBに関連する各種情報を取得する
 *
 *  \param   holder      : テーブルデータ構造体(実体)
 *  \param   dbTableConf : db_table_confマスタ情報
 *  \return  カラム、pk以外の値、pkのマップ、shard検索キー、エラー
 */
/**************************************************************************************************/
func (b *base) getTableInfoFromStructData(holder interface{}, dbTableConf *DbTableConf) ([]string, map[string]interface{}, builder.Eq, interface{}, error) {
	var err error

	var columns []string
	var shardKey interface{}

	var pkMap = builder.Eq{}
	var valueMap = map[string]interface{}{}

	// 実体の要素を把握する
	val := reflect.ValueOf(holder).Elem()
	for i := 0; i < val.NumField(); i++ {
		// 変数定義
		field := val.Type().Field(i)
		// タグ設定
		tag := field.Tag
		// 実値
		value := val.Field(i).Interface()

		// カラム
		column := strings.ToLower(field.Name)
		columns = append(columns, column)

		// PKは検索条件とし、それ以外は値を取得する
		if tag.Get("base") == "pk" {
			pkMap[column] = value
		} else {
			valueMap[column] = value
		}

		// shard keyを取得
		if dbTableConf.IsUseTypeShard() && tag.Get("shard") == "true" {
			// 2度設定はダメ
			if shardKey != nil {
				err = errors.New("multiple shard key not available!!")
				return columns, valueMap, pkMap, shardKey, err
			}
			shardKey = value
		}
	}

	// pkMapをチェックしておく
	if len(pkMap) < 1 {
		err = errors.New("must be set pks in struct!!")
		return columns, valueMap, pkMap, shardKey, err
	}

	return columns, valueMap, pkMap, shardKey, err
}

/**************************************************************************************************/
/*!
 *  shard keyからshard idを取得する
 *
 *  \param   holder      : テーブルデータ構造体(実体)
 *  \param   dbTableConf : db_table_confマスタ情報
 *  \return  カラム、pk以外の値、pkのマップ、shard検索キー、エラー
 */
/**************************************************************************************************/
func (b *base) getShardIdByShardKey(c *gin.Context, shardKey interface{}, dbTableConf *DbTableConf) (int, error) {
	var err error
	var shardId int

	// masterの場合は何もしない
	if dbTableConf.IsUseTypeMaster() {
		return shardId, err
	}

	// value check
	if shardKey == nil {
		err = errors.New("not set shard_key!!")
		return shardId, err
	}
	// 検索
	repo := NewShardRepo()
	shardId, err = repo.findShardId(c, dbTableConf.ShardType, shardKey)
	if err != nil {
		return shardId, err
	}
	return shardId, err
}

/**************************************************************************************************/
/*!
 *  type 各Condition(WHERE, ORDER BY)の構文解析を行う
 *
 *  \param   condition : where, orderに利用する条件
 *  \return  where文, where引数、orderBy用配列、エラー
 */
/**************************************************************************************************/
func (b *base) conditionCheck(condition map[string]interface{}) (string, []interface{}, []string, error) {
	var err error
	var whereSql string
	var whereArgs []interface{}
	var orders []string

	for k, v := range condition {
		switch k {
		case "where":
			// where条件解析
			whereSql, whereArgs, err = b.whereSyntaxAnalyze(v)
			if err != nil {
				log.Debug(err)
				return whereSql, whereArgs, orders, err
			}

		case "order":
			// order条件解析
			orders, err = b.orderSyntaxAnalyze(v)
			if err != nil {
				return whereSql, whereArgs, orders, err
			}

		default:
			err = errors.New("invalid condition type!!")
		}
	}
	return whereSql, whereArgs, orders, err
}

/**************************************************************************************************/
/*!
 *  Condition(WHERE)の構文解析を行う
 *
 *  使い方 :
 *  WhereCondition{
 *    {"column", "compare", value, ["AND/OR"]},
 *    ...,
 *  }
 *  column  : カラム名
 *  compare : 比較演算子("=", "<", ">", "<=", ">=", "IN", "LIKE")
 *  vauie   : 比較値
 *  AND/OR  : 次の条件式にANDかORで繋げる、省略時はAND
 *
 *  出力 :
 *  ORDER BY column1 ASC, column2 DESC
 *
 *  \param   i : WhereCondition型のinterface
 *  \return  where文, where引数、エラー
 */
/**************************************************************************************************/
const whereConditionMin = 3 //<! whereConditionの最小長
const whereConditionMax = 4 //<! whereConditionの最大長

func (b *base) whereSyntaxAnalyze(i interface{}) (string, []interface{}, error) {
	var err error
	var pred string
	var args []interface{}

	// 型チェック
	conds, ok := i.(WhereCondition)
	if !ok {
		err = errors.New("value is not where type!!")
		return pred, args, err
	}

	// {"column", "condition", "value", "AND/OR(option)"}
	lastIndex := len(conds) - 1
	var allSentence []string
	for index, cond := range conds {
		// 長さチェック
		length := len(cond)
		if !(whereConditionMin <= length && length <= whereConditionMax) {
			err = errors.New("where condition length error!! : " + strconv.Itoa(length))
			return pred, args, err
		}

		// 1 : column (型チェックのみ)
		column, ok := cond[0].(string)
		if !ok {
			err = errors.New("syntax error : column is string only!!")
			return pred, args, err
		}
		allSentence = append(allSentence, column)

		// 2 : 比較条件
		compare, ok := cond[1].(string)
		if !ok {
			err = errors.New("syntax error : compare is string only!!")
			return pred, args, err
		}

		isFind := false
		compares := []string{"=", "<", ">", "<=", ">=", "IN", "LIKE"}
		for _, v := range compares {
			if compare == v {
				isFind = true
				break
			}
		}
		if !isFind {
			err = errors.New("syntax error : this word can't use!! " + compare)
			return pred, args, err
		}
		allSentence = append(allSentence, compare)

		// 3 : 値
		if compare == "IN" {
			// プレースホルダを用意し、値をargsに入れる
			ifs := cond[2].(In)
			phs := []string{}
			for _, v := range ifs {
				phs = append(phs, "?")
				args = append(args, v)
			}
			// (?,?,?) の作成
			placeHolders := strings.Join(phs, ",")
			allSentence = append(allSentence, "("+placeHolders+")")
		} else {
			args = append(args, cond[2])
			allSentence = append(allSentence, "?")
		}

		// 4 : AND / OR (ない場合、ANDで結合)
		andOr := "AND"
		if length == whereConditionMax {
			c, ok := cond[3].(string)
			if !ok {
				err = errors.New("type error : this cond is and/or only!!")
				return pred, args, err
			}
			// 構文チェック
			if c != "AND" && c != "OR" {
				err = errors.New("syntax error : this cond is and/or only!!")
				return pred, args, err
			}
			andOr = c
		}

		// indexの最後以外は結合する
		if index != lastIndex {
			allSentence = append(allSentence, andOr)
		}
	}

	// すべてを結合
	pred = strings.Join(allSentence, " ")

	log.Debug(pred, " : ", args)

	return pred, args, err
}

/**************************************************************************************************/
/*!
 *  Condition(ORDER BY)の構文解析を行う
 *
 *  使い方 :
 *  OrderByCondition{
 *    {"column1", "ASC"},
 *    {"column2", "DESC"},
 *    ...,
 *  }
 *  出力 :
 *  ORDER BY column1 ASC, column2 DESC
 *
 *  \param   i : OrderByCondition型のinterface
 *  \return  orderBy用配列、エラー
 */
/**************************************************************************************************/
const orderCondition = 2 //<! orderConditionの長さ

func (b *base) orderSyntaxAnalyze(i interface{}) ([]string, error) {
	var err error
	var orders []string

	// 型チェック
	conds, ok := i.(OrderByCondition)
	if !ok {
		err = errors.New("value is not where type!!")
		return orders, err
	}

	// ["column", "ASC/DESC"]
	for _, cond := range conds {
		// 長さチェック
		length := len(cond)
		if length != orderCondition {
			err = errors.New("order condition length error!! : " + strconv.Itoa(length))
			return orders, err
		}
		// 構文チェック
		order := strings.Join(cond, " ")
		orders = append(orders, order)
	}

	return orders, err
}

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

		case Option:
			// 後で処理する
			optionMap = v.(Option)

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
