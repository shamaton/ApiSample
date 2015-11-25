package model

/**************************************************************************************************/
/*!
 *  base.go
 *
 *  テーブルデータ操作で基本的に使用されるベースモジュール
 *  但し、user_shard, db_table_confは特殊なため使わない
 *
 */
/**************************************************************************************************/
import (
	builder "github.com/Masterminds/squirrel"
	"github.com/gin-gonic/gin"

	"reflect"

	"sample/common/db"
	"sample/common/err"
	"sample/common/log"
	. "sample/conf"

	"strings"
)

// 一旦ここに
type Option map[string]interface{}
type Condition map[string]interface{}
type WhereCondition [][]interface{}
type OrderByCondition [][]string
type In []interface{}

/**
 * INSERT UPDATE系で除外するカラム
 */
const (
	createdAt = "created_at"
	updatedAt = "updated_at"
)

/**
 * sequence tableのprefix
 */
const seqTablePrefix = "seq_"

/**
 * interface
 */
type baseI interface {
	Find(*gin.Context, interface{}, ...interface{}) err.ErrWriter
	Finds(*gin.Context, interface{}, Condition, ...interface{}) err.ErrWriter

	Update(*gin.Context, interface{}, ...interface{}) err.ErrWriter
	Create(*gin.Context, interface{}) err.ErrWriter
	CreateMulti(*gin.Context, interface{}) err.ErrWriter

	Delete(*gin.Context, interface{}) err.ErrWriter

	Count(*gin.Context, Condition, ...interface{}) (int64, err.ErrWriter)
	Save(*gin.Context, interface{}) err.ErrWriter
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewBase(tableName string) baseI {
	return &base{table: tableName}
}

/**
 * entity
 */
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
 *  ex. Find(c, &struct, Option{...} )
 *
 *  \param   c       : コンテキスト
 *  \param   holder  : テーブルデータ構造体
 *  \param   options : モード[W,R,BAK] ロック[FOR_UPDATE]
 *  \return  エラー（正常時はholderにデータを取得する）
 */
/**************************************************************************************************/
func (b *base) Find(c *gin.Context, holder interface{}, options ...interface{}) err.ErrWriter {

	// optionsの解析
	mode, isForUpdate, _, _, ew := b.optionCheck(options...)
	if ew.HasErr() {
		return ew.Write("invalid options set!!")
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// holderから各要素を取得
	columns, _, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, false)
	if ew.HasErr() {
		return ew.Write("read error in struct data")
	}

	// shardの場合、shard_idを取得
	shardId, ew := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if ew.HasErr() {
		return ew.Write()
	}

	// SQL生成
	var sb builder.SelectBuilder
	columnStr := strings.Join(columns, ",")
	sb = builder.Select(columnStr).From(b.table).Where(pkMap)
	if isForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}
	sql, args, e := sb.ToSql()
	if e != nil {
		return ew.Write(e)
	}

	// fetch
	tx, ew := db.GetTransaction(c, mode, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write("transaction error!!")
	}
	e = tx.SelectOne(holder, sql, args...)
	if e != nil {
		return ew.Write("find error!!", e)
	}

	// TODO:デバッグでは通常selectで複数行取得されないことも確認する
	return ew
}

/**
 *  Finds method
 */
/**************************************************************************************************/
/*!
 *  指定テーブルへのselectを行う
 *
 *  ex. Finds(c, &[]struct, Condition{"where":WhereCondition, "order":OrderCondition{}}, Option{...} )
 *
 *  \param   c         : コンテキスト
 *  \param   holders   : select結果格納先
 *  \param   condition : where, orderに利用する条件
 *  \param   options   : モード[W,R,BAK] ロック[FOR_UPDATE]
 *  \return  エラー（正常時はholdersにデータを取得する）
 */
/**************************************************************************************************/
func (b *base) Finds(c *gin.Context, holders interface{}, condition Condition, options ...interface{}) err.ErrWriter {

	wSql, wArgs, orders, ew := b.conditionCheck(condition)
	if ew.HasErr() {
		return ew.Write("invalid condition set!!")
	}

	mode, _, shardKey, shardId, ew := b.optionCheck(options...) // isForUpdateは封印
	if ew.HasErr() {
		return ew.Write("invalid options set!!")
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// 構造体のポインタ配列(holder)からカラムを取得する
	// holdersは配列のポインタであること
	var structRef reflect.Type
	hRef := reflect.TypeOf(holders)
	if hRef.Kind() != reflect.Ptr {
		return ew.Write("holders type is not Ptr!!")
	}
	// 次にスライスであること
	sRef := hRef.Elem()
	if sRef.Kind() != reflect.Slice {
		return ew.Write("holders element type is not Slice!!")
	}
	// 最後に構造体であること
	structRef = sRef.Elem()
	if structRef.Kind() != reflect.Struct {
		return ew.Write("holders slice element type is not Struct!!")
	}

	// カラムの取得
	var columns []string
	for i := 0; i < structRef.NumField(); i++ {
		field := structRef.Field(i)

		var column string
		// タグがある場合は優先する
		if len(field.Tag.Get("db")) > 0 {
			column = field.Tag.Get("db")
		} else {
			column = strings.ToLower(field.Name)
		}
		columns = append(columns, column)
	}

	// shardIdをoptionで受け取ってないなら、shardKeyから取得する
	if shardId == 0 {
		shardId, ew = b.getShardIdByShardKey(c, shardKey, dbTableConf)
		if ew.HasErr() {
			return ew.Write()
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
	sql, args, e := sb.ToSql()
	if e != nil {
		return ew.Write(e)
	}

	// select
	tx, ew := db.GetTransaction(c, mode, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write("transaction error!!")
	}
	_, e = tx.Select(holders, sql, args...)
	if e != nil {
		return ew.Write("finds error!!", e)
	}

	return ew
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
 *  ex. Update(c, &struct, (&struct) )
 *
 *  \param   condition : where, orderに利用する条件
 *  \return  where文, where引数、orderBy用配列、エラー
 */
/**************************************************************************************************/
func (b *base) Update(c *gin.Context, holder interface{}, prevHolders ...interface{}) err.ErrWriter {

	// 過去データは1つしか想定してない
	if len(prevHolders) > 1 {
		return err.NewErrWriter("enable set 1 prevData only!!")
	}

	// 更新前のデータがある場合比較する
	// データの更新はないけど、データ更新がなかったという更新(update_at)のみしたい場合は...?
	for _, v := range prevHolders {
		nv := reflect.ValueOf(holder).Elem()
		pv := reflect.ValueOf(v).Elem()
		if nv.Interface() == pv.Interface() {
			// 更新の必要なし
			log.Info("this data is same.")
			return err.NewErrWriter()
		}
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// holderから各要素を取得
	_, valueMap, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, true)
	if ew.HasErr() {
		return ew.Write("read error in struct data")
	}

	// 更新前のデータがある場合、更新すべき値を抽出する
	for _, v := range prevHolders {
		pv := reflect.ValueOf(v).Elem()
		for i := 0; i < pv.NumField(); i++ {
			// 変数定義
			field := pv.Type().Field(i)
			// 実値
			value := pv.Field(i).Interface()

			// カラム
			column := strings.ToLower(field.Name)

			// mapに存在するものだけチェックしていく
			mv, ok := valueMap[column]
			if ok && mv == value {
				delete(valueMap, column)
				// 空になった時点で更新する必要なし
				if len(valueMap) < 1 {
					return ew
				}
			}
		}
	}

	// shardの場合、shard_idを取得
	shardId, ew := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if ew.HasErr() {
		return ew.Write()
	}

	// SQL生成
	sql, args, e := builder.Update(b.table).SetMap(valueMap).Where(pkMap).ToSql()
	if e != nil {
		return ew.Write("sql maker error!!", e)
	}
	// tx
	tx, ew := db.GetTransaction(c, MODE_W, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write()
	}
	// UPDATE(tx.updateはpkに対してまでsetするので使わない)
	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write("update error!!", e)
	}
	return ew
}

/**
 *  Create method
 */
/**************************************************************************************************/
/*!
 *  INSERT(IGNORE)を実行する
 *
 *  ex. Insert(c, &struct)
 *
 *  \param   c      : コンテキスト
 *  \param   holder : テーブルデータ構造体(実体)
 *  \return  処理失敗時エラー
 */
/**************************************************************************************************/
func (b *base) Create(c *gin.Context, holder interface{}) err.ErrWriter {

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// holderから各要素を取得
	columns, valueMap, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, true)
	if ew.HasErr() {
		return ew.Write("read error in struct data")
	}

	// shardの場合、shard_idを取得
	shardId, ew := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if ew.HasErr() {
		return ew.Write()
	}

	// TODO:pkのチェックするか検討

	// values収集
	var values []interface{}
	for _, column := range columns {
		if v, ok := pkMap[column]; ok {
			values = append(values, v)
		} else if v, ok := valueMap[column]; ok {
			values = append(values, v)
		} else {
			return ew.Write("unknown column found!!")
		}
	}

	// SQL生成
	columnStr := strings.Join(columns, ",")
	sql, args, e := builder.Insert(b.table).Options("IGNORE").Columns(columnStr).Values(values...).ToSql()
	//sql, args, err := builder.Insert(b.table).Values(values...).ToSql()
	if e != nil {
		return ew.Write("sql maker error!!", e)
	}
	// tx
	tx, ew := db.GetTransaction(c, MODE_W, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write()
	}
	// CREATE(tx.Insertは要マッピングなので使わない)
	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write("create error!!", e)
	}
	return ew
}

/**
 * Create Multi method
 */
/**************************************************************************************************/
/*!
 *  データ構造体配列からINSERT MULTIを実行する
 *
 *  &(array)[ struct{}, struct{}, ...] のようなデータを想定している
 *
 *  \param   c      : コンテキスト
 *  \param   holder : テーブルデータ構造体配列
 *  \return  カラム、pk以外の値、pkのマップ、shard検索キー、エラー
 */
/**************************************************************************************************/
func (b *base) CreateMulti(c *gin.Context, holders interface{}) err.ErrWriter {

	// 参照渡しかチェック
	hRef := reflect.ValueOf(holders)
	if hRef.Kind() != reflect.Ptr {
		return err.NewErrWriter("holders type is not Ptr!!")
	}

	// スライスかチェック
	sRef := hRef.Elem()
	if sRef.Kind() != reflect.Slice {
		return err.NewErrWriter("holders Ptr type is not slice!!")
	}

	length := sRef.Len()
	// 空チェック
	if length < 1 {
		return err.NewErrWriter("holder slice invalid length!!")
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// テーブルの情報を取得
	var shardIdMap = map[int]int{} // for check
	var shardId int
	var allValues [][]interface{}
	var columnStr string
	for i := 0; i < length; i++ {
		holder := sRef.Index(i).Interface()

		columns, valueMap, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, true)
		if ew.HasErr() {
			return ew.Write("read error in struct data")
		}

		// values収集
		var values []interface{}
		for _, column := range columns {
			if v, ok := pkMap[column]; ok {
				values = append(values, v)
			} else if v, ok := valueMap[column]; ok {
				values = append(values, v)
			} else {
				return ew.Write("unknown column found!!")
			}
		}
		allValues = append(allValues, values)

		// shardの場合、shard_idを取得
		shardId, ew = b.getShardIdByShardKey(c, shardKey, dbTableConf)
		shardIdMap[shardId] = shardId
		if ew.HasErr() {
			return ew.Write()
		}

		// 初回だけ作成
		if len(columnStr) < 1 {
			columnStr = strings.Join(columns, ",")
		}
	}

	// 取得されたshardIDはユニークであること
	if len(shardIdMap) != 1 {
		return ew.Write("can not set multi shard id !!")
	}

	// SQL生成
	var ib builder.InsertBuilder
	ib = builder.Insert(b.table).Options("IGNORE").Columns(columnStr)

	// Valuesで接続する
	for _, values := range allValues {
		ib = ib.Values(values...)
	}

	sql, args, e := ib.ToSql()
	if e != nil {
		return ew.Write("sql maker error!!", e)
	}
	// tx
	tx, ew := db.GetTransaction(c, MODE_W, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write()
	}

	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write("create multi error!!", e)
	}

	return ew
}

/**
 *  Delete method
 */
/**************************************************************************************************/
/*!
 *  PRIMARY KEYを利用したDELETEを行う
 *
 *  ex. Delete(c, &{struct})
 *
 *  \param   c      : コンテキスト
 *  \param   holder : テーブルデータ構造体
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
func (b *base) Delete(c *gin.Context, holder interface{}) err.ErrWriter {

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// holderから各要素を取得
	_, _, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, false)
	if ew.HasErr() {
		return ew.Write("read error in struct data")
	}

	// shardの場合、shard_idを取得
	shardId, ew := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if ew.HasErr() {
		return ew.Write()
	}

	// SQL生成
	sql, args, e := builder.Delete(b.table).Where(pkMap).ToSql()
	if e != nil {
		return ew.Write("sql maker error!!", e)
	}
	// tx
	tx, ew := db.GetTransaction(c, MODE_W, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write("transaction error!!")
	}
	// DELETE
	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write(e)
	}
	return ew
}

/**
 *  Save method
 */
/**************************************************************************************************/
/*!
 *  DBにレコードが存在していればUPDATEし、なければCREATEする
 *
 *  Saveは多用せず、なるべくCREATE/UPDATEを明示して利用すること
 *  ex. Save(c, &{struct})
 *
 *  \param   c      : コンテキスト
 *  \param   holder : テーブルデータ構造体
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
// TODO : createと共通化
func (b *base) Save(c *gin.Context, holder interface{}) err.ErrWriter {
	ew := err.NewErrWriter()

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return ew.Write()
	}

	// holderから各要素を取得
	columns, valueMap, pkMap, shardKey, ew := b.getTableInfoFromStructData(c, holder, dbTableConf, true)
	if ew.HasErr() {
		return ew.Write("read error in struct data")
	}

	// shardの場合、shard_idを取得
	shardId, ew := b.getShardIdByShardKey(c, shardKey, dbTableConf)
	if ew.HasErr() {
		return ew.Write()
	}

	// TODO:pkのチェックするか検討

	// values収集
	var values []interface{}
	var dupCols []string
	var dupValues []interface{}

	// NOTE : マップで回すとカラムの順序がおかしくなる
	for _, column := range columns {
		if v, ok := pkMap[column]; ok {
			values = append(values, v)
		} else if v, ok := valueMap[column]; ok {
			values = append(values, v)
			dupCols = append(dupCols, column+" = ?")
			dupValues = append(dupValues, v)
		} else {
			return err.NewErrWriter("unknown column found!!")
		}
	}

	// DUPLICATE文作成
	dupStr := strings.Join(dupCols, ", ")
	suffix := strings.Join([]string{"ON DUPLICATE KEY UPDATE", dupStr}, " ")

	// SQL生成
	columnStr := strings.Join(columns, ",")
	sql, args, e := builder.Insert(b.table).Columns(columnStr).Values(values...).Suffix(suffix, dupValues...).ToSql()
	if e != nil {
		return ew.Write("sql maker error!!", e)
	}
	// tx
	tx, ew := db.GetTransaction(c, MODE_W, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return ew.Write("transaction error!!")
	}
	// UPDATE(tx.Insertは要マッピングなので使わない)
	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write("update failed!!", e)
	}
	return ew
}

/**
 *  Count Method
 */
/**************************************************************************************************/
/*!
 *  指定条件でレコードをCOUNTする
 *
 *  ex. Count(c, Condition{"where":WhereCondition, "order":OrderCondition{}}, Option{...} )
 *
 *  \param   c         : コンテキスト
 *  \param   condition : where, orderに利用する条件
 *  \param   options   : Option Map
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
func (b *base) Count(c *gin.Context, condition Condition, options ...interface{}) (int64, err.ErrWriter) {
	var count int64
	ew := err.NewErrWriter()

	wSql, wArgs, orders, ew := b.conditionCheck(condition)
	if ew.HasErr() {
		return count, ew.Write("invalid condition set!!")
	}

	mode, _, shardKey, shardId, ew := b.optionCheck(options...)
	if ew.HasErr() {
		return count, ew.Write("invalid options set!!")
	}

	// db_table_confから属性を把握
	dbTableConfRepo := NewDbTableConfRepo()
	dbTableConf, ew := dbTableConfRepo.Find(c, b.table)
	if ew.HasErr() {
		return count, ew.Write()
	}

	// shardIdをoptionで受け取ってないなら、shardKeyから取得する
	if shardId == 0 {
		shardId, ew = b.getShardIdByShardKey(c, shardKey, dbTableConf)
		if ew.HasErr() {
			return count, ew.Write()
		}
	}

	// SQL生成
	var sb builder.SelectBuilder

	sb = builder.Select("COUNT(1)").From(b.table)
	if len(wSql) > 0 && len(wArgs) > 0 {
		sb = sb.Where(wSql, wArgs...)
	}
	if len(orders) > 0 {
		sb = sb.OrderBy(orders...)
	}

	sql, args, e := sb.ToSql()
	if e != nil {
		return count, ew.Write("sql error!!", e)
	}

	// select
	tx, ew := db.GetTransaction(c, mode, dbTableConf.IsUseTypeShard(), shardId)
	if ew.HasErr() {
		return count, ew.Write("transaction error!!")
	}
	count, e = tx.SelectInt(sql, args...)
	if e != nil {
		return count, ew.Write("select count failed!!", e)
	}

	return count, ew
}

/**************************************************************************************************/
/*!
 *  データ構造体からDBに関連する各種情報を取得する
 *
 *  \param   holder      : テーブルデータ構造体(実体)
 *  \param   dbTableConf : db_table_confマスタ情報
 *  \param   isINSorUPD  : INSERT or UPDATE時にtrue
 *  \return  カラム、pk以外の値、pkのマップ、shard検索キー、エラー
 */
/**************************************************************************************************/
func (b *base) getTableInfoFromStructData(
	c *gin.Context, holder interface{}, dbTableConf *DbTableConf, isINSorUPD bool,
) ([]string, map[string]interface{}, builder.Eq, interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()

	var columns []string
	var shardKey interface{}

	var pkMap = builder.Eq{}
	var valueMap = map[string]interface{}{}

	var val reflect.Value
	// ポインタの場合、構造体の実体を取得する
	if reflect.ValueOf(holder).Kind() == reflect.Ptr {
		val = reflect.ValueOf(holder).Elem()
	} else {
		val = reflect.ValueOf(holder)
	}

	// 実体の要素を把握する
	for i := 0; i < val.NumField(); i++ {
		// 変数定義
		field := val.Type().Field(i)
		// タグ設定
		tag := field.Tag
		// 実値
		value := val.Field(i).Interface()

		// カラム
		var column string
		// タグがある場合は優先する
		if len(tag.Get("db")) > 0 {
			column = tag.Get("db")
		} else {
			column = strings.ToLower(field.Name)
		}

		// INSERT, UPDATEではupdated_atとcreated_atを除外する
		if isINSorUPD && (column == createdAt || column == updatedAt) {
			continue
		}
		// シーケンシャルIDで値が設定されてない場合は採番する
		if isINSorUPD && tag.Get("seq") == "t" {
			if value.(uint64) < 1 {
				value, ew = b.getSeqId(c)
				if ew.HasErr() {
					return columns, valueMap, pkMap, shardKey, ew.Write()
				}
			}
		}

		columns = append(columns, column)

		// PKは検索条件とし、それ以外は値を取得する
		if tag.Get("pk") == "t" {
			pkMap[column] = value
		} else {
			valueMap[column] = value
		}

		// shard keyを取得
		if dbTableConf.IsUseTypeShard() && tag.Get("shard") == "t" {
			// 2度設定はダメ
			if shardKey != nil {
				return columns, valueMap, pkMap, shardKey, ew.Write("multiple shard key not available!!")
			}
			shardKey = value
		}
	}

	// pkMapをチェックしておく
	if len(pkMap) < 1 {
		return columns, valueMap, pkMap, shardKey, ew.Write("must be set pks in struct!!")
	}

	return columns, valueMap, pkMap, shardKey, ew
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
func (b *base) getShardIdByShardKey(c *gin.Context, shardKey interface{}, dbTableConf *DbTableConf) (int, err.ErrWriter) {
	ew := err.NewErrWriter()
	var shardId int

	// masterの場合は何もしない
	if dbTableConf.IsUseTypeMaster() {
		return shardId, ew
	}

	// value check
	if shardKey == nil {
		return shardId, ew.Write("not set shard_key!!")
	}
	// 検索
	repo := NewShardRepo()
	shardId, ew = repo.FindShardId(c, dbTableConf.ShardType, shardKey)
	if ew.HasErr() {
		return shardId, ew.Write()
	}
	return shardId, ew
}

/**************************************************************************************************/
/*!
 *  type 各Condition(WHERE, ORDER BY)の構文解析を行う
 *
 *  \param   condition : where, orderに利用する条件
 *  \return  where文, where引数、orderBy用配列、エラー
 */
/**************************************************************************************************/
func (b *base) conditionCheck(condition map[string]interface{}) (string, []interface{}, []string, err.ErrWriter) {
	ew := err.NewErrWriter()
	var whereSql string
	var whereArgs []interface{}
	var orders []string

	for k, v := range condition {
		switch k {
		case "where":
			// where条件解析
			whereSql, whereArgs, ew = b.whereSyntaxAnalyze(v)
			if ew.HasErr() {
				return whereSql, whereArgs, orders, ew.Write()
			}

		case "order":
			// order条件解析
			orders, ew = b.orderSyntaxAnalyze(v)
			if ew.HasErr() {
				return whereSql, whereArgs, orders, ew.Write()
			}

		default:
			ew.Write("invalid condition type!!")
		}
	}
	return whereSql, whereArgs, orders, ew
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

func (b *base) whereSyntaxAnalyze(i interface{}) (string, []interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()
	var pred string
	var args []interface{}

	// 型チェック
	conds, ok := i.(WhereCondition)
	if !ok {
		return pred, args, ew.Write("value is not where type!!")
	}

	// {"column", "condition", "value", "AND/OR(option)"}
	lastIndex := len(conds) - 1
	var allSentence []string
	for index, cond := range conds {
		// 長さチェック
		length := len(cond)
		if !(whereConditionMin <= length && length <= whereConditionMax) {
			return pred, args, ew.Write("where condition length error!! : ", length)
		}

		// 1 : column (型チェックのみ)
		column, ok := cond[0].(string)
		if !ok {
			return pred, args, ew.Write("syntax error : column is string only!!")
		}
		allSentence = append(allSentence, column)

		// 2 : 比較条件
		compare, ok := cond[1].(string)
		if !ok {
			return pred, args, ew.Write("syntax error : compare is string only!!")
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
			return pred, args, ew.Write("syntax error : this word can't use!! " + compare)
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
				return pred, args, ew.Write("type error : this cond is and/or only!!")
			}
			// 構文チェック
			if c != "AND" && c != "OR" {
				return pred, args, ew.Write("syntax error : this cond is and/or only!!")
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

	return pred, args, ew
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

func (b *base) orderSyntaxAnalyze(i interface{}) ([]string, err.ErrWriter) {
	ew := err.NewErrWriter()
	var orders []string

	// 型チェック
	conds, ok := i.(OrderByCondition)
	if !ok {
		return orders, ew.Write("value is not where type!!")
	}

	// ["column", "ASC/DESC"]
	for _, cond := range conds {
		// 長さチェック
		length := len(cond)
		if length != orderCondition {
			return orders, ew.Write("order condition length error!! : ", length)
		}
		// 構文チェック
		order := strings.Join(cond, " ")
		orders = append(orders, order)
	}

	return orders, ew
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
func (b *base) optionCheck(options ...interface{}) (string, bool, interface{}, int, err.ErrWriter) {
	ew := err.NewErrWriter()

	var mode = MODE_R
	var isForUpdate = false
	var shardKey interface{}
	var shardId int

	var optionMap Option

	for _, v := range options {

		switch v.(type) {
		case Option:
			// 後で処理する
			optionMap = v.(Option)

		default:
			return mode, isForUpdate, shardKey, shardId, ew.Write("can not check this type!!")
		}
	}

	// optionMapの解析
	// TODO:専用のtypeを作成する
	for k, v := range optionMap {

		switch k {
		case "mode":
			str := v.(string)
			if str == MODE_W || str == MODE_R || str == MODE_BAK {
				mode = str
			} else {
				return mode, isForUpdate, shardKey, shardId, ew.Write("invalid mode!!")
			}

		case "for_update":
			isForUpdate = true

		case "shard_key":
			shardKey = v

		case "shard_id":
			value, isInt := v.(int)
			// 型チェック & 範囲チェック
			if !isInt {
				return mode, isForUpdate, shardKey, shardId, ew.Write("type not integer!!")
			} else if value < 1 || value > 2 {
				// TODO:ちゃんとチェックする
				return mode, isForUpdate, shardKey, shardId, ew.Write("over shard id range!!")
			}
			shardId = v.(int)

		default:
			return mode, isForUpdate, shardKey, shardId, ew.Write("invalid key!!")

		}
	}

	// shard系は1つのみを想定する
	if shardKey != nil && shardId > 0 {
		return mode, isForUpdate, shardKey, shardId, ew.Write("can't set shardKey and shardId in optionMap!!")
	}

	// for updateな場合、MODEは必ずW
	if isForUpdate {
		mode = MODE_W
	}
	return mode, isForUpdate, shardKey, shardId, ew
}

/**************************************************************************************************/
/*!
 *  SEQUENCE TABLEからシーケンスIDを取得する
 *
 *  \param   c : コンテキスト
 *  \return  シーケンスID、エラー
 */
/**************************************************************************************************/
func (b *base) getSeqId(c *gin.Context) (uint64, err.ErrWriter) {
	seqIds, ew := b.getSeqIds(c, 1)
	if ew.HasErr() {
		return 0, ew.Write()
	}
	seqId := seqIds[0]

	return seqId, ew
}

/**************************************************************************************************/
/*!
 *  SEQUENCE TABLEからシーケンスIDを取得する
 *
 *  \param   c      : コンテキスト
 *  \param   getNum : 採番したい数
 *  \return  シーケンスID、エラー
 */
/**************************************************************************************************/
func (b *base) getSeqIds(c *gin.Context, getNum uint64) ([]uint64, err.ErrWriter) {
	// seqテーブルは必ずmaster
	isShard, shardId := false, 0

	// validate
	if getNum < 1 {
		return nil, err.NewErrWriter("invalid getNum : ", getNum)
	}

	// tx get
	tx, ew := db.GetTransaction(c, MODE_W, isShard, shardId)
	if ew.HasErr() {
		return nil, ew.Write()
	}

	// table lock
	seqTableName := seqTablePrefix + b.table
	_, e := tx.Exec("LOCK TABLES " + seqTableName + " WRITE")
	if e != nil {
		return nil, err.NewErrWriter("lock tables error : "+seqTableName, e)
	}

	// update and select
	_, e = tx.Exec("UPDATE "+seqTableName+" set id = id + ?", getNum)
	if e != nil {
		return nil, err.NewErrWriter("update seq table error : "+seqTableName, e)
	}

	var seqId uint64
	e = tx.SelectOne(&seqId, "select max(id) from "+seqTableName)
	if e != nil {
		return nil, err.NewErrWriter("seq select error : "+seqTableName, e)
	}

	// table unlock
	_, e = tx.Exec("UNLOCK TABLES")
	if e != nil {
		return nil, err.NewErrWriter("unlock tables error : "+seqTableName, e)
	}

	// sedIds生成
	var seqIds []uint64
	var i uint64
	for i = 0; i < getNum; i++ {
		seqIds = append([]uint64{seqId - i}, seqIds...)
	}

	return seqIds, err.NewErrWriter()
}
