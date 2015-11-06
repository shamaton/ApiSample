package model2

import (
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/xorm"
	"sample/DBI"
)

// table
type User struct {
	Id    int    `xorm:"pk"`
	Name  string `xorm:"pk"`
	Score int
}

type UserRepo interface {
	FindByID(int) (*User, error)
}

func NewUserRepo(c *gin.Context) UserRepo {
	tx, _ := DBI.GetDBSession(c)
	db, _ := DBI.GetDBConnection(c, "user")

	return UserRepoImpl{db: db, tx: tx}
}

type UserRepoImpl struct {
	db *xorm.Engine
	tx *xorm.Session
}

func (r UserRepoImpl) FindByID(id int) (*User, error) {
	user := new(User)
	var err error

	user.Id = id
	_, err = r.db.Get(user)

	return user, err
}
