package model

import (
	"github.com/gin-gonic/gin"
)

type User struct {
	Id    int `base:"pk" shard:"true"`
	Name  string
	Score int
}

// user
/////////////////////////////
type UserRepo interface {
	FindByID(*gin.Context, int, ...interface{}) (*User, error)

	Update(map[string]interface{})
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
