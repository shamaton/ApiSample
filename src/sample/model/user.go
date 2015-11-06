package model

import (
	"sample/DBI"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

type User struct {
	Id    int    `xorm:"pk"`
	Name  string `xorm:"pk"`
	Score int
	//Hoge int32   //`db:"score, [primarykey, autoincrement]"` 変数名とカラム名が異なる場合JSON的に書ける
}

type UserTable struct {
	*User
	*modelBase
}

var (
	m modelBase = modelBase{shard: true}
)

func Find(c *gin.Context, userId int, options ...interface{}) (User, error) {
	var user User

	h, err := DBI.GetDBConnection(c, "user", options...)
	if err != nil {
		return user, err
	}

	// データをselect
	user.Id = userId
	_, err = h.Get(&user)

	//var user User
	//_, err := h.Id(userId).Get(&user)

	return user, err

}

func FindForUpdate(c *gin.Context, userId int, options ...interface{}) (User, error) {
	var user User

	tx, err := DBI.GetDBSession(c)
	if err != nil {
		return user, err
	}

	var u []User
	err = tx.Where("id = ?", userId).ForUpdate().Find(&u)
	if err != nil {
		log.Error(err)
		return user, err
	}

	user = u[0]

	return user, err
}
