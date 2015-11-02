package hoge

import (
	"log"

	"conf/gameConf"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"golang.org/x/net/context"
	"strconv"
)

var dbMasterW *xorm.Engine
var dbMasterR *xorm.Engine
var dbShardWMap map[int]*xorm.Engine
var dbShardRMap map[int]*xorm.Engine

var shardIds = [...]int{1, 2}

func BuildInstances(ctx context.Context) {
	var err error

	gameConf := ctx.Value("gameConf").(*gameConf.GameConfig)

	// mapは初期化されないので注意
	dbShardWMap = map[int]*xorm.Engine{}
	dbShardRMap = map[int]*xorm.Engine{}

	master_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		gameConf.Db.User,
		gameConf.Db.Pass,
		gameConf.Server.Host,
		gameConf.Server.Port,
		"game_master")

	// master_master
	dbMasterW, err = xorm.NewEngine("mysql", master_dsn)
	checkErr(err, "master instance failed!!")

	// master_shard
	for i := 0; i < 2; i++ {
		shard_id := i + 1
		shard_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			gameConf.Db.User,
			gameConf.Db.Pass,
			gameConf.Server.Host,
			gameConf.Server.Port,
			"game_shard_"+strconv.Itoa(shard_id))
		dbShardWMap[shard_id], err = xorm.NewEngine("mysql", shard_dsn)
		checkErr(err, "shard "+strconv.Itoa(shard_id)+" instance failed!!")
	}

	// slave
	// TODO : 複数台対応
	dbMasterR, err = xorm.NewEngine("mysql", master_dsn)
	checkErr(err, "master instance failed!!")

	// master_shard
	for i := 0; i < 2; i++ {
		shard_id := i + 1
		shard_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			gameConf.Db.User,
			gameConf.Db.Pass,
			gameConf.Server.Host,
			gameConf.Server.Port,
			"game_shard_"+strconv.Itoa(shard_id))
		dbShardRMap[shard_id], err = xorm.NewEngine("mysql", shard_dsn)
		checkErr(err, "shard "+strconv.Itoa(shard_id)+" instance failed!!")
	}
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
