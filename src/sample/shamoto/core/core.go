package core

import (
	"database/sql"

	log "github.com/cihub/seelog"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/gorp.v1"
)

type User struct {
	Id    int32
	Name  string
	Score int32
	//Hoge int32   //`db:"score, [primarykey, autoincrement]"` 変数名とカラム名が異なる場合JSON的に書ける
}

var testDbmap *gorp.DbMap
var tx *gorp.Transaction

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

func StartTx() {
	tx, _ = testDbmap.Begin()
}

func Commit() {
	tx.Commit()
	tx = nil
}

func RollBack() {
	tx.Rollback()
	tx = nil
}
