package model

/**
 * dbTableConfテーブルアクセサ
 */

import (
	builder "github.com/Masterminds/squirrel"
	"github.com/gin-gonic/gin"

	"sample/common/db"
	"sample/common/err"
	. "sample/conf"
)

/**
 * master or shard
 */
const (
	useTypeNone int = iota
	useTypeMaster
	useTypeShard
)

/**
 * シャーディングタイプ(どのようにshardingされているか)
 */
const (
	shardTypeNone int = iota
	shardTypeUser
	shardTypeGroup
)

/**
 * \struct DbTableConf
 * \brief テーブル定義
 */
type DbTableConf struct {
	Id        int
	TableName string `db:"table_name"`
	UseType   int    `db:"use_type"`
	ShardType int    `db:"shard_type"`
}

/**
 * interface
 */
type dbTableConfRepoI interface {
	Find(*gin.Context, string) (*DbTableConf, err.ErrWriter)
}

/**
 * db accessor
 */
type dbTableConfRepo struct {
	table   string
	columns string
	cacheI
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewDbTableConfRepo() dbTableConfRepoI {
	cacheRepo := NewCacheRepo()
	repo := &dbTableConfRepo{
		table:   "db_table_conf",
		columns: "id, table_name, use_type, shard_type",
		cacheI:  cacheRepo,
	}
	return repo
}

/**************************************************************************************************/
/*!
 *  DbTableConfデータの取得
 *
 *  \param   c : コンテキスト
 *  \param   tableName : 探すテーブル名
 *  \return  テーブルデータ、エラー
 */
/**************************************************************************************************/
func (this *dbTableConfRepo) Find(c *gin.Context, tableName string) (*DbTableConf, err.ErrWriter) {

	cv, ew := this.GetCacheWithSetter(c, this.cacheSetter, this.table, "all")
	if ew.HasErr() {
		return nil, ew.Write()
	}
	allData := cv.(map[string]DbTableConf)

	data, isValid := allData[tableName]
	if !isValid {
		return nil, ew.Write("not found db_table_conf record!!")
	}

	return &data, ew
}

/**************************************************************************************************/
/*!
 *  DbTableConfデータの全取得
 *
 *  \param   c : コンテキスト
 *  \return  全データ、エラー
 */
/**************************************************************************************************/
func (this *dbTableConfRepo) finds(c *gin.Context) (*[]DbTableConf, err.ErrWriter) {
	var datas []DbTableConf

	// ハンドル取得
	tx, ew := db.GetTransaction(c, MODE_R, false, 0)
	if ew.HasErr() {
		return nil, ew.Write("not found master connection!!")
	}

	// user_shardを検索
	sql, args, e := builder.Select(this.columns).From(this.table).ToSql()
	if e != nil {
		return nil, ew.Write("query build error!!")
	}

	_, e = tx.Select(&datas, sql, args...)
	if e != nil {
		return nil, ew.Write("not found db table conf!!")
	}

	return &datas, ew
}

/**************************************************************************************************/
/*!
 *  キャッシュを生成してセット
 *
 *  \param   c         : コンテキスト
 *  \return  cacheGetしたものと同等のデータ、エラー
 */
/**************************************************************************************************/
func (this *dbTableConfRepo) cacheSetter(c *gin.Context) (interface{}, err.ErrWriter) {
	allData, ew := this.finds(c)
	if ew.HasErr() {
		return nil, ew.Write()
	}
	if len(*allData) < 1 {
		return nil, ew.Write("db_table_conf is empty!!")
	}

	// マップ生成
	dataMap := map[string]DbTableConf{}
	for _, v := range *allData {
		dataMap[v.TableName] = v
	}
	this.SetCache(dataMap, this.table, "all")

	return dataMap, ew
}

/**
 * table data method
 */
/**************************************************************************************************/
/*!
 *  マスタデータか
 *  \return  true or false
 */
/**************************************************************************************************/
func (d *DbTableConf) IsUseTypeMaster() bool {
	if d.UseType == useTypeMaster {
		return true
	}
	return false
}

/**************************************************************************************************/
/*!
 *  シャードデータか
 *  \return  true or false
 */
/**************************************************************************************************/
func (d *DbTableConf) IsUseTypeShard() bool {
	if d.UseType == useTypeShard {
		return true
	}
	return false
}

/**************************************************************************************************/
/*!
 *  USER_IDでシャーディングされているか
 *  \return  true or false
 */
/**************************************************************************************************/
func (d *DbTableConf) IsShardTypeUser() bool {
	if d.ShardType == shardTypeUser {
		return true
	}
	return false
}

/**************************************************************************************************/
/*!
 *  GROUP_IDでシャーディングされているか
 *  \return  true or false
 */
/**************************************************************************************************/
func (d *DbTableConf) IsShardTypeGroup() bool {
	if d.ShardType == shardTypeGroup {
		return true
	}
	return false
}
