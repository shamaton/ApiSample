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
	builder "github.com/Masterminds/squirrel"
	"github.com/gin-gonic/gin"

	"sample/common/db"
	"sample/common/log"

	"sample/common/err"
	. "sample/conf"
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
type userShardRepoI interface {
	FindByUserId(*gin.Context, interface{}, ...interface{}) (*UserShard, err.ErrWriter)

	Create(*gin.Context, *UserShard) err.ErrWriter
}

/**
 * db accessor
 */
type userShardRepo struct {
	table   string
	columns string
	cacheI
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserShardRepo() userShardRepoI {
	cache := NewCacheRepo()
	repo := &userShardRepo{
		table:   "user_shard",
		columns: "id, shard_id",
		cacheI:  cache,
	}
	return repo
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
func (this *userShardRepo) Create(c *gin.Context, userShard *UserShard) err.ErrWriter {
	// SQL生成
	sql, args, e := builder.Insert("user_shard").Options("IGNORE").Values(userShard.Id, userShard.ShardId).ToSql()
	if e != nil {
		return err.NewErrWriter("sql maker error!!", e)
	}

	// get master tx
	tx, ew := db.GetTransaction(c, MODE_W, false, 0)
	if ew.HasErr() {
		return ew.Write("transaction error!!")
	}

	// create
	_, e = tx.Exec(sql, args...)
	if e != nil {
		return ew.Write("create user shard error!!", e)
	}

	return ew
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
func (this *userShardRepo) FindByUserId(c *gin.Context, userId interface{}, options ...interface{}) (*UserShard, err.ErrWriter) {
	var userShard UserShard

	// optionsの解析
	b := base{}
	mode, _, _, _, ew := b.optionCheck(options...)
	if ew.HasErr() {
		return nil, ew.Write("invalid options set!!")
	}

	if mode == MODE_W {
		// ハンドル取得
		conn, ew := db.GetDBMasterConnection(c, mode)
		if ew.HasErr() {
			return nil, ew.Write("not found master connection!!")
		}

		// クエリ生成
		sql, args, e := builder.Select(this.columns).From(this.table).Where("id = ?", userId).ToSql()
		if e != nil {
			return nil, ew.Write("query build error!!", e)
		}

		// user_shardを検索
		e = conn.SelectOne(&userShard, sql, args...)
		if e != nil {
			return nil, ew.Write("select shard error!!", e)
		}
		// ユーザー生成していない場合があるので、エラーにはしない
		if userShard.ShardId < 1 {
			log.Info("not found user shard id")
		}
	} else {
		cv, ew := this.GetCacheWithSetter(c, this.cacheSetter, this.table, "all")
		if ew.HasErr() {
			return nil, ew.Write()
		}
		allData := cv.(map[int]UserShard)
		data, ok := allData[int(userId.(uint64))]

		// キャッシュにない場合、DBから探す
		if !ok {
			dbData, ew := this.FindByUserId(c, userId, Option{"mode": MODE_W})

			// それでもダメならエラー
			if ew.HasErr() || dbData.ShardId < 1 {
				return nil, ew.Write()
			}

			// 更新しておく
			userShard = *dbData
			allData[data.Id] = userShard
			this.SetCache(allData, this.table, "all")
		} else {
			userShard = data
		}
	}

	return &userShard, ew
}

/**************************************************************************************************/
/*!
 *  データ全取得
 *
 *  \param   c : コンテキスト
 *  \return  全データ、エラー
 */
/**************************************************************************************************/
func (this *userShardRepo) finds(c *gin.Context, mode string) (*[]UserShard, err.ErrWriter) {
	// ハンドル取得
	conn, ew := db.GetDBMasterConnection(c, mode)
	if ew.HasErr() {
		return nil, ew.Write("not found master connection!!")
	}

	// クエリ生成
	sql, args, e := builder.Select("id, shard_id").From("user_shard").ToSql()
	if e != nil {
		return nil, ew.Write("query build error!!", e)
	}

	// 全取得
	var allData []UserShard
	_, e = conn.Select(&allData, sql, args...)
	if e != nil {
		return nil, ew.Write("select shard error!!", e)
	}
	return &allData, ew
}

/**************************************************************************************************/
/*!
 *  キャッシュを生成してセット
 *
 *  \param   c         : コンテキスト
 *  \return  cacheGetしたものと同等のデータ、エラー
 */
/**************************************************************************************************/
func (this *userShardRepo) cacheSetter(c *gin.Context) (interface{}, err.ErrWriter) {
	allData, ew := this.finds(c, MODE_R)
	if ew.HasErr() {
		return nil, ew.Write()
	}
	// マップ生成
	dataMap := map[int]UserShard{}
	for _, v := range *allData {
		dataMap[v.Id] = v
	}
	this.SetCache(dataMap, this.table, "all")

	return dataMap, ew
}
