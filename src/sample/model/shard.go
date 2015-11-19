package model

/**************************************************************************************************/
/*!
 *  \file shard.go
 *  shard関連制御
 */
/**************************************************************************************************/

import (
	"errors"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

/**
 * interface
 */
type shardRepoI interface {
	FindShardId(*gin.Context, int, interface{}, ...interface{}) (int, error)
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewShardRepo() shardRepoI {
	return &shardRepo{}
}

type shardRepo struct {
}

/**************************************************************************************************/
/*!
 *  shard idを取得する
 *
 *  \param   c : コンテキスト
 *  \param   st : shardType
 *  \return  shard ID、エラー
 */
/**************************************************************************************************/
func (r *shardRepo) FindShardId(c *gin.Context, st int, value interface{}, options ...interface{}) (int, error) {
	var shardId int
	var err error

	switch st {
	case shardTypeUser:
		userShardRepo := NewUserShardRepo()
		userShard, err := userShardRepo.FindByUserId(c, value, options...)
		if err != nil {
			log.Error("error : find by user...")
			return shardId, err
		}
		shardId = userShard.ShardId

	//case shardTypeGroup:
	// TODO:実装

	default:
		err = errors.New("undefined shard type!!")
	}

	return shardId, err
}
