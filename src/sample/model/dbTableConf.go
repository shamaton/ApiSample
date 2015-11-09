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
 * テーブル名
 */
var table = "db_table_conf"

const (
	shardTypeNone int = iota
	shardTypeMaster
	shardTypeShard
)

/**
 * @struct DbTableConf
 * @brief テーブル定義
 */
type DbTableConf struct {
	Id        int
	TableName string `db:"table_name"`
	ShardType int    `db:"shard_type"`
}

/**
 * table data method
 */
/**************************************************************************************************/
/*!
 *  \fn      public bool IsShardTypeMaster()
 *           マスタデータか
 *  \return  true or false
 */
/**************************************************************************************************/
func (data *DbTableConf) IsShardTypeMaster() bool {
	if data.ShardType == shardTypeMaster {
		return true
	}
	return false
}

/**************************************************************************************************/
/*!
 *  \fn      public bool IsShardTypeShard()
 *           シャードデータか
 *  \return  true or false
 */
/**************************************************************************************************/
func (data *DbTableConf) IsShardTypeShard() bool {
	if data.ShardType == shardTypeShard {
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
	sql, args, err := builder.Select("id, table_name, shard_type").From(table).Where("table_name = ?", tableName).ToSql()
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
