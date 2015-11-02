package hoge

import (
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
)

var dbMasterW *xorm.Engine
var dbMasterR *xorm.Engine
var dbShardWMap map[int]*xorm.Engine
var dbShardRMap map[int]*xorm.Engine

var shardIds = [...]int{1, 2}

func BuildInstances() {
	var err error

	// mapは初期化されないので注意
	dbShardWMap = map[int]*xorm.Engine{}
	dbShardRMap = map[int]*xorm.Engine{}

	// master
	dbMasterW, err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_master?charset=utf8")
	checkErr(err, "master instance failed!!")
	dbShardWMap[1], err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_shard_1?charset=utf8")
	checkErr(err, "shard 1 instance failed!!")
	dbShardWMap[2], err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_shard_2?charset=utf8")
	checkErr(err, "shard 2 instance failed!!")

	// slave
	dbMasterR, err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_master?charset=utf8")
	checkErr(err, "master instance failed!!")
	dbShardRMap[1], err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_shard_1?charset=utf8")
	checkErr(err, "shard 1 instance failed!!")
	dbShardRMap[2], err = xorm.NewEngine("mysql", "game:game@tcp(localhost:3306)/game_shard_2?charset=utf8")
	checkErr(err, "shard 2 instance failed!!")
}

// 仮。これはリクエストキャッシュに持つ。
var txMap map[int]*xorm.Session

func StartTx() {
	txMap = map[int]*xorm.Session{}
	// txのマップを作成
	for k, v := range dbShardWMap {
		log.Println(k, " start tx!!")
		txMap[k] = v.NewSession()
	}
	// errを返す
}

func Commit() {
	for k, v := range txMap {
		log.Println(k, " commit!!")
		/*err :=*/ v.Commit()
		txMap[k] = nil
	}
	// errを返す
}

func RollBack() {
	for k, v := range txMap {
		log.Println(k, " commit!!")
		/*err :=*/ v.Rollback()
		txMap[k] = nil
	}
	// errを返す
}

func GetDBShardConnection(shard_type string, value int) *xorm.Engine {
	shardId := 1
	return dbShardWMap[shardId]
}

func GetTxByShardKey(shard_type string, value int) *xorm.Session {
	shardId := 1
	return txMap[shardId]
}

// エラー表示
func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
	}
}
