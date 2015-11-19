package model

/**************************************************************************************************/
/*!
 *  user.go
 *
 *  table : userのmodelクラス
 *
 */
/**************************************************************************************************/
import (
	"time"

	"github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

/**
 * \struct User
 * \brief テーブル定義
 */
type User struct {
	Id        uint64 `pk:"t" shard:"t"`
	Name      string
	Score     uint
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserRepo() *userRepo {
	b := NewBase("user")
	return &userRepo{b}
}

/**
 * Implementer
 */
type userRepo struct {
	baseI
}

/**************************************************************************************************/
/*!
 *  ユーザーIDで検索する
 *
 *  \param   c       : コンテキスト
 *  \param   id      : ユーザーID
 *  \param   options : オプション
 *  \return  ユーザーデータ(エラー時はnil)
 */
/**************************************************************************************************/
func (r *userRepo) FindById(c *gin.Context, id uint64, options ...interface{}) *User {
	var user = new(User)
	user.Id = id
	err := r.Find(c, user, options...)
	if err != nil {
		return nil
	}
	return user
}

func (this *userRepo) FindsTest(c *gin.Context) {
	var users []User

	whereCond := WhereCondition{
		{"id", "<=", 1, "OR"},
		{"id", ">", 2},
		//{"id", "IN", In{1, 2, 3, 4}},
		//{"name", "LIKE", "%aaa%"},
	}
	orderCond := OrderByCondition{
		{"id", "ASC"},
		{"score", "ASC"},
	}
	var condition = Condition{"where": whereCond, "order": orderCond}

	var option = Option{"shard_id": 1}

	this.Finds(c, &users, condition, option)
	seelog.Debug(&users)

	var hoges []User
	this.Finds(c, &hoges, Condition{}, option)
	seelog.Debug(&hoges)
}
