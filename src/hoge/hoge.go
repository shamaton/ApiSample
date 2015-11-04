package hoge

import (
	"conf/gameConf"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"golang.org/x/net/context"
	"math/rand"
	"strconv"

	"errors"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

var (
	dbMasterW    *xorm.Engine
	dbMasterR    *xorm.Engine
	dbShardWMap  map[int]*xorm.Engine
	dbShardRMaps []map[int]*xorm.Engine

	slaveWeights []int

	shardIds = [...]int{1, 2}
)

const (
	MASTER = iota
	SHARD
)

const (
	MODE_W   = iota // master
	MODE_R          // slave
	MODE_BAK        // backup
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
	for slave_index, slaveConf := range gameConf.Server.Slave {
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

		// slaveの選択比重
		for i := 0; i < slaveConf.Weight; i++ {
			slaveWeights = append(slaveWeights, slave_index)
		}
	}

}

func StartTx(c *gin.Context) {
	var txMap = map[int]*xorm.Session{}
	// txのマップを作成
	for k, v := range dbShardWMap {
		log.Info(k, " start tx!!")
		txMap[k] = v.NewSession()
	}
	c.Set("txMap", txMap)
	// errを返す
}

func Commit(c *gin.Context) {
	txMap := c.Value("txMap").(map[int]*xorm.Session)
	for k, v := range txMap {
		log.Info(k, " commit!!")
		/*err :=*/ v.Commit()
		// txMap[k] = nil
	}
	c.Set("txMap", nil)
	// errを返す
}

func RollBack(c *gin.Context) {
	txMap := c.Value("txMap").(map[int]*xorm.Session)
	for k, v := range txMap {
		log.Info(k, " commit!!")
		/*err :=*/ v.Rollback()
		// txMap[k] = nil
	}
	c.Set("txMap", nil)
	// errを返す
}

func GetDBConnection(c *gin.Context, tableName string) (*xorm.Engine, error) {
	var err error
	// db_conf_tableからshardかmasterを取得
	dbType := SHARD // shard

	var conn *xorm.Engine
	// masterの場合
	switch dbType {
	case MASTER:
		conn = dbMasterR
	case SHARD:
		slaveIndex := c.Value("slaveIndex").(int)
		shardMap := dbShardRMaps[slaveIndex]
		// TODO:仮
		shardId := 1
		conn = shardMap[shardId]

	default:
		err = errors.New("undefined db type!!")
	}

	// shardの場合
	if conn == nil {
		err = errors.New("not found db connection!!")
	}
	return conn, err
}

func GetDBSession(c *gin.Context) (*xorm.Session, error) {
	isTxStart := c.Value("isTxStart").(bool)

	// TODO:仮
	shardId := 1

	var err error
	var tx *xorm.Session

	// セッションを開始してない場合はエラーとしておく
	if isTxStart {
		sMap := c.Value("txMap").(map[int]*xorm.Session)
		tx = sMap[shardId]
	} else {
		err = errors.New("transaction not started!!")
	}

	return tx, err
}

// 使うslaveを決める
func DecideUseSlave() int {
	slaveIndex := rand.Intn(len(slaveWeights))
	return slaveWeights[slaveIndex]
}

// エラー表示
func checkErr(err error, msg string) {
	if err != nil {
		log.Error(msg, err)
	}
}
