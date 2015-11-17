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

type UserItem struct {
	UserId    uint64 `db:"user_id" pk:"t" shard:"t"`
	ItemId    int    `db:"item_id" pk:"t"`
	Num       int
	UsedNum   int       `db:"used_num"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

/**
 * Interface
 */
type UserItemRepo interface {
	FindByPk(*gin.Context, uint64, int, ...interface{}) *UserItem

	Save(*gin.Context, interface{}) error
	Delete(*gin.Context, interface{}) error
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserItemRepo() UserItemRepo {
	b := &base{table: "user_item"}
	return UserRepoImpl{b}
}

/**
 * Implementer
 */
type UserItemRepoImpl struct {
	*base
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
func (r UserRepoImpl) FindByPk(c *gin.Context, userId uint64, itemId int, options ...interface{}) *UserItem {
	userItem := &UserItem{UserId: userId, ItemId: itemId}
	err := r.Find(c, userItem, options...)
	if err != nil {
		return nil
	}
	return userItem
}
