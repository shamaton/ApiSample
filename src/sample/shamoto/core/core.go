package core

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/gorp.v1"
	log "github.com/cihub/seelog"
)

type User struct {
	Id int32
	Name  string
	Score int32
	//Hoge int32   //`db:"score, [primarykey, autoincrement]"` 変数名とカラム名が異なる場合JSON的に書ける
}

var testDbmap *gorp.DbMap

func InitDb() error {
	var err error
	// MySQLへのハンドラ
	db, err := sql.Open("mysql", "game:game@tcp(localhost:3306)/game_shard_1")
	if err != nil {
		log.Critical(err)
	}

	// construct a gorp DbMap
	testDbmap = &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}


	return err
}

func GetDB() *gorp.DbMap {
	return testDbmap
}

