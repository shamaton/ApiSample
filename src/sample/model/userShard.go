package model

/**************************************************************************************************/
/*!
 *  userShard.go
 *
 *  table : user_shardのmodelクラス
 *
 */
/**************************************************************************************************/
import (
	"sample/DBI"
	"sample/cache"

	builder "github.com/Masterminds/squirrel"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)


/**
 * \struct UserShard
 * \brief テーブル定義
 */
type UserShard struct {
	Id      int
	ShardId int `db:"shard_id"`
}


/**
 * Interface
 */
type UserShardRepo interface {
	FindByUserId(*gin.Context, interface{}, ...interface{}) (*UserShard, error)

	Create(*gin.Context, *UserShard) error
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserShardRepo() UserShardRepo {
	return UserShardRepoImpl{table: "user_shard"}
}

/**
 * Implementer
 */
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

/**************************************************************************************************/
/*!
 *  データ全取得
 *
 *  \param   c : コンテキスト
 *  \return  全データ、エラー
 */
/**************************************************************************************************/
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

/**************************************************************************************************/
/*!
 *  キャッシュ生成
 *
 *  \param   c         : コンテキスト
 *  \return  cacheGetしたものと同等のデータ、エラー
 */
/**************************************************************************************************/
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

/**************************************************************************************************/
/*!
 *  シャードデータ作成
 *
 *  \param   c         : コンテキスト
 *  \param   userShard : データ構造体
 *  \return  失敗時、エラー
 */
/**************************************************************************************************/
func (impl UserShardRepoImpl) Create(c *gin.Context, userShard *UserShard) error {
	// SQL生成
	sql, args, err := builder.Insert("user_shard").Options("IGNORE").Values(userShard.Id, userShard.ShardId).ToSql()
	if err != nil {
		log.Error("sql maker error!!")
		return err
	}

	// get master tx
	tx, err := DBI.GetTransaction(c, DBI.MODE_W, false, 0)
	if err != nil {
		log.Error("transaction error!!")
		return err
	}

	// create
	log.Critical(sql, args)
	_, err = tx.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}
