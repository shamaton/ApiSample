package model

/**
 * dbTableConfテーブルアクセサ
 */

import (
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"

	"errors"
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

// CACHE TEST
var dbTableConfCache = map[string]DbTableConf{}

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
func (impl DbTableConfRepoImpl) Find(c *gin.Context, tableName string) (*DbTableConf, error) {
	var err error

	if len(dbTableConfCache) < 1 {
		err = impl.makeCache(c)
		if err != nil {
			return nil, err
		}
	}

	data, isValid := dbTableConfCache[tableName]
	if !isValid {
		err = errors.New("not found db_table_conf record!!")
		return nil, err
	}

	return &data, err
}

/**************************************************************************************************/
/*!
 *  DbTableConfデータの全取得
 *
 *  \param   c : コンテキスト
 *  \return  全データ、エラー
 */
/**************************************************************************************************/
func (impl DbTableConfRepoImpl) finds(c *gin.Context) (*[]DbTableConf, error) {
	var datas []DbTableConf

	// ハンドル取得
	conn, err := DBI.GetDBMasterConnection(c, DBI.MODE_R)
	if err != nil {
		log.Error("not found master connection!!")
		return nil, err
	}

	// user_shardを検索
	sql, args, err := builder.Select(columns).From(table).ToSql()
	if err != nil {
		log.Error("query build error!!")
		return nil, err
	}

	_, err = conn.Select(&datas, sql, args...)
	if err != nil {
		log.Error("not found db table conf!!")
	}

	return &datas, err
}

/**************************************************************************************************/
/*!
 *  キャッシュを生成する
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func (impl DbTableConfRepoImpl) makeCache(c *gin.Context) error {
	allData, err := impl.finds(c)
	if err != nil {
		log.Error("db_table_conf err!!")
		return err
	}
	if len(*allData) < 1 {
		err = errors.New("db_table_conf is empty!!")
		log.Error(err)
		return err
	}

	// mapを生成する
	for _, v := range *allData {
		dbTableConfCache[v.TableName] = v
	}
	return err
}
