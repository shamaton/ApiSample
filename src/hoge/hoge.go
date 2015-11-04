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

var (
	dbMasterW    *xorm.Engine
	dbMasterR    *xorm.Engine
	dbShardWMap  map[int]*xorm.Engine
	dbShardRMaps []map[int]*xorm.Engine

	shardWeightMap map[int]int

	shardIds = [...]int{1, 2}
)

func BuildInstances(ctx context.Context) {
	var err error

	gameConf := ctx.Value("gameConf").(*gameConf.GameConfig)

	// mapは初期化されないので注意
	dbShardWMap = map[int]*xorm.Engine{}

	master_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		gameConf.Db.User,
		gameConf.Db.Pass,
		gameConf.Server.Host,
		gameConf.Server.Port,
		"game_master")

	// master_master
	dbMasterW, err = xorm.NewEngine("mysql", master_dsn)
	checkErr(err, "masterDB master instance failed!!")

	// master_shard
	for _, shard_id := range shardIds {
		shard_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			gameConf.Db.User,
			gameConf.Db.Pass,
			gameConf.Server.Host,
			gameConf.Server.Port,
			"game_shard_"+strconv.Itoa(shard_id))
		dbShardWMap[shard_id], err = xorm.NewEngine("mysql", shard_dsn)
		checkErr(err, "master shard "+strconv.Itoa(shard_id)+" instance failed!!")

	}

	// slave_master
	shard_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		gameConf.Db.User,
		gameConf.Db.Pass,
		gameConf.Server.Host,
		gameConf.Server.Port,
		"game_master")

	dbMasterR, err = xorm.NewEngine("mysql", shard_dsn)
	checkErr(err, "slaveDB master instance failed!!")

	// slave_shard
	for _, slaveConf := range gameConf.Server.Slave {
		var shardMap = map[int]*xorm.Engine{}

		for _, shard_id := range shardIds {
			dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
				gameConf.Db.User,
				gameConf.Db.Pass,
				slaveConf.Host,
				gameConf.Server.Port,
				"game_shard_"+strconv.Itoa(shard_id))

			// create instance
			shardMap[shard_id], err = xorm.NewEngine("mysql", dsn)
			checkErr(err, "slave shard"+strconv.Itoa(shard_id)+" instance failed!!")
		}
		dbShardRMaps = append(dbShardRMaps, shardMap)
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
