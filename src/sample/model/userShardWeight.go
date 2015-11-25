package model

/**************************************************************************************************/
/*!
 *  userShardWeight.go
 *
 *  table : user_shard_weightのmodelクラス
 *
 */
/**************************************************************************************************/
import (
	builder "github.com/Masterminds/squirrel"
	"github.com/gin-gonic/gin"

	"sample/common/db"

	"sample/common/err"
	. "sample/conf"
	"math/rand"
	"sample/common/log"
)

/**
 * \struct UserShard
 * \brief テーブル定義
 */
type UserShardWeight struct {
	ShardId int `db:"shard_id" pk:"true"`
	Weight int
}

/**
 * Interface
 */
type userShardWeightRepoI interface {
	ChoiceShardId(*gin.Context) (int, err.ErrWriter)
}

/**
 * db accessor
 */
type userShardWeightRepo struct {
	table   string
	columns string
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserShardWeightRepo() userShardWeightRepoI {
	repo := &userShardWeightRepo{
		table:   "user_shard_weight",
		columns: "shard_id, weight",
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
func (this *userShardWeightRepo) ChoiceShardId(c *gin.Context) (int, err.ErrWriter) {
	// 重みを全て取得
	allData, ew := this.finds(c, MODE_R)
	if ew.HasErr() {
		return 0, ew.Write()
	}

	// 配列を作成
	var shardWeights []int
	for _, v := range *allData {
		for i := 0; i < v.Weight; i++ {
			shardWeights = append(shardWeights, v.ShardId)
		}
	}

	log.Info(shardWeights)

	// ランダムに抽選
	index := rand.Intn(len(shardWeights))
	shardId := shardWeights[index]

	// 一応チェック
	if shardId < 1 {
		return 0, ew.Write("shard id : 0 is invalid!!")
	}

	return shardId, ew
}

/**************************************************************************************************/
/*!
 *  データ全取得
 *
 *  \param   c : コンテキスト
 *  \return  全データ、エラー
 */
/**************************************************************************************************/
func (this *userShardWeightRepo) finds(c *gin.Context, mode string) (*[]UserShardWeight, err.ErrWriter) {
	// ハンドル取得
	conn, ew := db.GetDBMasterConnection(c, mode)
	if ew.HasErr() {
		return nil, ew.Write("not found master connection!!")
	}

	// クエリ生成
	sql, args, e := builder.Select(this.columns).From(this.table).ToSql()
	if e != nil {
		return nil, ew.Write("query build error!!", e)
	}

	// 全取得
	var allData []UserShardWeight
	_, e = conn.Select(&allData, sql, args...)
	if e != nil {
		return nil, ew.Write("select shard weight error!!", e)
	}

	if len(allData) < 1 {
		return nil, ew.Write("user shard weight record empty!!")
	}

	return &allData, ew
}
