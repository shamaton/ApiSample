package model

import (
	"github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

type User struct {
	Id        int `pk:"true" shard:"true"`
	Name      string
	Score     int
	CreatedAt uint `db:"created_at"`
	UpdatedAt uint `db:"updated_at"`
}

// user
/////////////////////////////
type UserRepo interface {
	FindByID(*gin.Context, int, ...interface{}) (*User, error)

	Update(*gin.Context, interface{}, ...interface{}) error
	Create(*gin.Context, interface{}) error

	// test
	FindsTest(*gin.Context)
}

func NewUserRepo() UserRepo {
	b := &base{table: "user"}
	return UserRepoImpl{b}
}

type UserRepoImpl struct {
	*base
}

func (r UserRepoImpl) FindByID(c *gin.Context, id int, options ...interface{}) (*User, error) {
	var user = new(User)
	user.Id = id
	err := r.Find(c, user, options...)
	return user, err
}

func (r UserRepoImpl) FindsTest(c *gin.Context) {
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

	r.Finds(c, &users, condition, option)
	seelog.Debug(&users)

	var hoges []User
	r.Finds(c, &hoges, Condition{}, option)
	seelog.Debug(&hoges)
}
