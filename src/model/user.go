package model

import (
	"DBI"
	"log"

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

func Find(c *gin.Context, userId int) User {
	h, err := DBI.GetDBConnection(c, "user", DBI.MODE_R)

	// データをselect
	var user = User{Id: userId}
	_, err = h.Get(&user)

	//var user User
	//_, err := h.Id(userId).Get(&user)

	checkErr(err, "not found data!")
	return user

}

// エラー表示
func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}
