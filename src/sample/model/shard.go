package model

/**************************************************************************************************/
/*!
 *  \file shard.go
 *  shard関連制御
 */
/**************************************************************************************************/

import (
	"sample/common/err"

	"reflect"
	"sample/common/db"

	"github.com/gin-gonic/gin"
)

/**
 * interface
 */
type shardRepoI interface {
	FindShardId(*gin.Context, int, interface{}, ...interface{}) (int, err.ErrWriter)
}

/**
 * db accessor
 */
type shardRepo struct {
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewShardRepo() shardRepoI {
	return &shardRepo{}
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
func (r *shardRepo) FindShardId(c *gin.Context, st int, value interface{}, options ...interface{}) (int, err.ErrWriter) {
	var shardId int
	ew := err.NewErrWriter()

	switch st {
	case shardTypeUser:
		userShardRepo := NewUserShardRepo()
		userShard, ew := userShardRepo.FindByUserId(c, value, options...)
		if ew.HasErr() {
			return shardId, ew.Write("error : find by user...")
		}
		shardId = userShard.ShardId

	case shardTypeGroup:
		// とりあえず数値を想定しておく
		ref := reflect.ValueOf(value)
		if ref.Kind() == reflect.Int {
			v := value.(int)
			shardId = (v % len(db.GetShardIds())) + 1
		} else {
			ew.Write("type not int!!")
		}

	default:
		ew.Write("undefined shard type!!")
	}

	return shardId, ew
}
