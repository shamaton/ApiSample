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
 *
 */

type ShardRepo interface {
	findShardId(*gin.Context, int, interface{}, ...interface{}) (int, error)
}

func NewShardRepo() ShardRepo {
	return ShardRepoImpl{}
}

type ShardRepoImpl struct {
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
func (r ShardRepoImpl) findShardId(c *gin.Context, st int, value interface{}, options ...interface{}) (int, error) {
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
