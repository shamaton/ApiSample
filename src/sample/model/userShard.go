package model

/**
 * @file userShard.go
 * @brief userShardテーブル操作
 */

import (
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
type UserShardRepo interface {
	FindByUserId(*gin.Context, interface{}, ...interface{}) (*UserShard, error)
}

func NewUserShardRepo() UserShardRepo {
	return UserShardRepoImpl{}
}

type UserShardRepoImpl struct {
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
func (r UserShardRepoImpl) FindByUserId(c *gin.Context, userId interface{}, options ...interface{}) (*UserShard, error) {
	var err error
	userShard := new(UserShard)

	// optionsの解析
	b := base{}
	mode, _, _, _, err := b.optionCheck(options...)
	if err != nil {
		log.Error("invalid options set!!")
		return userShard, err
	}

	// ハンドル取得
	conn, err := DBI.GetDBMasterConnection(c, mode)
	if err != nil {
		log.Error("not found master connection!!")
		return userShard, err
	}

	// クエリ生成
	sql, args, err := builder.Select("id, shard_id").From("user_shard").Where("id = ?", userId).ToSql()
	if err != nil {
		log.Error("query build error!!")
		return userShard, err
	}

	// user_shardを検索
	err = conn.SelectOne(userShard, sql, args...)
	if err != nil {
		return userShard, err
	}
	// ユーザー生成していない場合があるので、エラーにはしない
	if userShard.ShardId < 1 {
		log.Info("not found user shard id")
	}

	return userShard, err
}
