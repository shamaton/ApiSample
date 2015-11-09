package model

import (
	builder "github.com/Masterminds/squirrel"
	"github.com/gin-gonic/gin"
)

type User struct {
	Id    int32
	Name  string
	Score int32
	//Hoge int32   //`db:"score, [primarykey, autoincrement]"` 変数名とカラム名が異なる場合JSON的に書ける
}

// user
/////////////////////////////
type UserRepo interface {
	FindByID(*gin.Context, int) (*User, error)
}

func NewUserRepo() UserRepo {
	b := &base{}
	return UserRepoImpl{b}
}

type UserRepoImpl struct {
	*base
}

func (r UserRepoImpl) FindByID(c *gin.Context, id int) (*User, error) {
	var user = new(User)
	sb := builder.Select("id, name, score").From("user").Where("id = ?", id)
	err := r.Find(user, sb)
	return user, err
}
