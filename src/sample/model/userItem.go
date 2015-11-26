package model

/**************************************************************************************************/
/*!
 *  userItem.go
 *
 *  table : user_itemのmodelクラス
 *
 */
/**************************************************************************************************/
import (
	"time"

	"github.com/gin-gonic/gin"
)

/**
 * \struct UserItem
 * \brief テーブル定義
 */
type UserItem struct {
	UserId    uint64 `db:"user_id" pk:"t" shard:"t"`
	ItemId    int    `db:"item_id" pk:"t"`
	Num       int
	UsedNum   int       `db:"used_num"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

/**
 * db accessor
 */
type userItemRepo struct {
	BaseI
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserItemRepo() *userItemRepo {
	b := NewBase("user_item")
	return &userItemRepo{b}
}

/**************************************************************************************************/
/*!
 *  PRIMARY KEYで検索する
 *
 *  \param   c       : コンテキスト
 *  \param   userId  : ユーザーID
 *  \param   itemId  : アイテムID
 *  \param   options : オプション
 *  \return  ユーザーアイテムデータ(エラー時はnil)
 */
/**************************************************************************************************/
func (this *userItemRepo) FindByPk(c *gin.Context, userId uint64, itemId int, options ...interface{}) *UserItem {
	userItem := &UserItem{UserId: userId, ItemId: itemId}
	ew := this.Find(c, userItem, options...)
	if ew.HasErr() {
		return nil
	}
	return userItem
}
