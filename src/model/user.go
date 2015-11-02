package model

import (
	"hoge"
	"log"

	"github.com/go-xorm/xorm"
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

func Find(userId int) User {
	var h *xorm.Engine
	h = hoge.GetDBShardConnection("user", 1)

	// データをselect
	var user = User{Id: userId}
	_, err := h.Get(&user)

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
