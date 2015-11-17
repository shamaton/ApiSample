package model

/**
 * @file userShard.go
 * @brief userShardテーブル操作
 */

import (
	"sample/DBI"
	"sample/cache"

	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
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
	return UserShardRepoImpl{table: "user_shard"}
}

type UserShardRepoImpl struct {
	table string
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
	var userShard UserShard

	// optionsの解析
	b := base{}
	mode, _, _, _, err := b.optionCheck(options...)
	if err != nil {
		log.Error("invalid options set!!")
		return nil, err
	}

	if mode == DBI.MODE_W {
		// ハンドル取得
		conn, err := DBI.GetDBMasterConnection(c, mode)
		if err != nil {
			log.Error("not found master connection!!")
			return nil, err
		}

		// クエリ生成
		sql, args, err := builder.Select("id, shard_id").From(r.table).Where("id = ?", userId).ToSql()
		if err != nil {
			log.Error("query build error!!")
			return nil, err
		}

		// user_shardを検索
		err = conn.SelectOne(&userShard, sql, args...)
		if err != nil {
			return nil, err
		}
		// ユーザー生成していない場合があるので、エラーにはしない
		if userShard.ShardId < 1 {
			log.Info("not found user shard id")
		}
	} else {
		cv, err := cache.Get(r.table, "all")
		if err != nil {
			return nil, err
		}
		if cv == nil {
			cv, err = r.makeCache(c)
			if err != nil {
				return nil, err
			}
		}
		allData := cv.(map[int]UserShard)
		userShard = allData[int(userId.(uint64))]
	}

	return &userShard, err
}

func (impl UserShardRepoImpl) finds(c *gin.Context) (*[]UserShard, error) {
	// ハンドル取得
	conn, err := DBI.GetDBMasterConnection(c, DBI.MODE_R)
	if err != nil {
		log.Error("not found master connection!!")
		return nil, err
	}

	// クエリ生成
	sql, args, err := builder.Select("id, shard_id").From("user_shard").ToSql()
	if err != nil {
		log.Error("query build error!!")
		return nil, err
	}

	// 全取得
	var allData []UserShard
	_, err = conn.Select(&allData, sql, args...)
	if err != nil {
		return nil, err
	}
	return &allData, nil
}

func (impl UserShardRepoImpl) makeCache(c *gin.Context) (interface{}, error) {
	allData, err := impl.finds(c)
	if err != nil {
		return nil, err
	}
	// マップ生成
	dataMap := map[int]UserShard{}
	for _, v := range *allData {
		dataMap[v.Id] = v
	}
	cache.Set(impl.table, "all", dataMap)
	return dataMap, nil
}
