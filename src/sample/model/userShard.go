package model

/**
 * @file userShard.go
 * @brief userShardテーブル操作
 */

import (
	"errors"
	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"sample/DBI"
)

/**
 *
 */

// table
type UserShard struct {
	Id      int
	ShardId int `db:"shard_id"`
}

// user shard
/////////////////////////////
type ShardRepo interface {
	FindShardIdByUserId(*gin.Context, interface{}) (int, error)

	findShardId(*gin.Context, int, interface{}) (int, error)
}

func NewShardRepo() ShardRepo {
	return ShardRepoImpl{}
}

type ShardRepoImpl struct {
}

/**************************************************************************************************/
/*!
 *  ユーザーIDの紐づくshard idを取得する
 *
 *  \param   c : コンテキスト
 *  \param   userId : ユーザーID
 *  \return  shard ID、エラー
 */
/**************************************************************************************************/
func (r ShardRepoImpl) FindShardIdByUserId(c *gin.Context, userId interface{}) (int, error) {
	shardId, err := r.findShardId(c, shardTypeUser, userId)
	return shardId, err
}

//
func (r ShardRepoImpl) findShardId(c *gin.Context, st int, value interface{}) (int, error) {
	var shardId int
	var err error

	switch st {
	case shardTypeUser:
		// ハンドル取得
		conn, err := DBI.GetDBMasterConnection(c, DBI.MODE_R)
		if err != nil {
			log.Error("not found master connection!!")
			break
		}

		// user_shardを検索
		sql, args, err := builder.Select("id, shard_id").From("user_shard").Where("id = ?", value).ToSql()
		if err != nil {
			log.Error("query build error!!")
			break
		}

		var userShard = new(UserShard)
		err = conn.SelectOne(userShard, sql, args...)
		if err != nil {
			log.Info("not found user shard id")
			break
		}
		shardId = userShard.ShardId

	//case shardTypeGroup:
	// TODO:実装

	default:
		err = errors.New("undefined shard type!!")
	}

	return shardId, err
}
