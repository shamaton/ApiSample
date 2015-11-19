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

// CACHE TEST
var dbTableConfCache = map[string]DbTableConf{}

/**
 * interface
 */
type dbTableConfRepoI interface {
	Find(*gin.Context, string) (*DbTableConf, error)
}

/**
 * db accessor function
 */
/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewDbTableConfRepo() dbTableConfRepoI {
	repo := &dbTableConfRepo{
		table:   "db_table_conf",
		columns: "id, table_name, use_type, shard_type",
	}
	return repo
}

type dbTableConfRepo struct {
	table   string
	columns string
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
func (this *dbTableConfRepo) Find(c *gin.Context, tableName string) (*DbTableConf, error) {
	var err error

	if len(dbTableConfCache) < 1 {
		err = this.makeCache(c)
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
func (this *dbTableConfRepo) finds(c *gin.Context) (*[]DbTableConf, error) {
	var datas []DbTableConf

	// ハンドル取得
	conn, err := DBI.GetDBMasterConnection(c, DBI.MODE_R)
	if err != nil {
		log.Error("not found master connection!!")
		return nil, err
	}

	// user_shardを検索
	sql, args, err := builder.Select(this.columns).From(this.table).ToSql()
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
func (this *dbTableConfRepo) makeCache(c *gin.Context) error {
	allData, err := this.finds(c)
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
