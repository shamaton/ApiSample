package model

/**
 * dbTableConfテーブルアクセサ
 */

import (
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"

	"sample/DBI"
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
	//shardTypeGroup
)

/**
 * @struct DbTableConf
 * @brief テーブル定義
 */
type DbTableConf struct {
	Id        int
	TableName string `db:"table_name"`
	UseType   int    `db:"use_type"`
	ShardType int    `db:"shard_type"`
}

var table = "db_table_conf"
var columns = "id, table_name, use_type, shard_type"

/**
 * table data method
 */
/**************************************************************************************************/
/*!
 *  マスタデータか
 *  \return  true or false
 */
/**************************************************************************************************/
func (data *DbTableConf) IsUseTypeMaster() bool {
	if data.UseType == useTypeMaster {
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
func (data *DbTableConf) IsUseTypeShard() bool {
	if data.UseType == useTypeShard {
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
func (data *DbTableConf) IsShardTypeUser() bool {
	if data.ShardType == shardTypeUser {
		return true
	}
	return false
}

/**
 * database accessor method
 */
type DbTableConfRepo interface {
	Find(*gin.Context, string) (*DbTableConf, error)
}

func NewDbTableConfRepo() DbTableConfRepo {
	return DbTableConfRepoImpl{}
}

type DbTableConfRepoImpl struct {
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
func (r DbTableConfRepoImpl) Find(c *gin.Context, tableName string) (*DbTableConf, error) {
	var err error
	var row = new(DbTableConf)

	// ハンドル取得
	conn, err := DBI.GetDBMasterConnection(c, DBI.MODE_R)
	if err != nil {
		log.Error("not found master connection!!")
		return row, err
	}

	// user_shardを検索
	sql, args, err := builder.Select(columns).From(table).Where("table_name = ?", tableName).ToSql()
	if err != nil {
		log.Error("query build error!!")
		return row, err
	}

	err = conn.SelectOne(row, sql, args...)
	if err != nil {
		log.Error("not found db table conf!!")
	}

	return row, err
}
