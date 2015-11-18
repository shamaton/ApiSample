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

/**
 * Interface
 */
type UserRepo interface {
	Create(*gin.Context, *User) error
	CreateMulti(*gin.Context, *[]User) error
	Update(*gin.Context, *User, ...interface{}) error
	Save(*gin.Context, *User) error

	Delete(*gin.Context, *User) error

	Count(*gin.Context, Condition, ...interface{}) (int64, error)

	// test
	FindById(*gin.Context, uint64, ...interface{}) *User
	FindsTest(*gin.Context)
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserRepo() UserRepo {
	b := NewBase("user")
	return &userRepo{base: b}
}

/**
 * Implementer
 */
type userRepo struct {
	base Base
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
	err := r.base.Find(c, user, options...)
	if err != nil {
		return nil
	}
	return user
}

func (r *userRepo) FindsTest(c *gin.Context) {
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

	r.base.Finds(c, &users, condition, option)
	seelog.Debug(&users)

	var hoges []User
	r.base.Finds(c, &hoges, Condition{}, option)
	seelog.Debug(&hoges)
}

/**************************************************************************************************/
/*!
 *  以下、基本メソッド
 */
/**************************************************************************************************/
func (this *userRepo) Create(c *gin.Context, user *User) error {
	return this.base.Create(c, user)
}

func (this *userRepo) Delete(c *gin.Context, user *User) error {
	return this.base.Delete(c, user)
}

func (this *userRepo) CreateMulti(c *gin.Context, users *[]User) error {
	return this.base.CreateMulti(c, users)
}

func (this *userRepo) Save(c *gin.Context, user *User) error {
	return this.base.Save(c, user)
}

func (this *userRepo) Update(c *gin.Context, user *User, prev ...interface{}) error {
	return this.base.Update(c, user, prev...)
}

func (this *userRepo) Count(c *gin.Context, condition Condition, options ...interface{}) (int64, error) {
	return this.base.Count(c, condition, options...)
}