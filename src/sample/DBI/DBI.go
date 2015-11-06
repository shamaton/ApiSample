package DBI

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"golang.org/x/net/context"
	"math/rand"
	"sample/conf/gameConf"
	"strconv"

	"errors"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

var (
	slaveWeights []int

	shardIds = [...]int{1, 2}
)

const (
	MASTER = iota
	SHARD
)

const (
	MODE_W   = "W"   // master
	MODE_R   = "R"   // slave
	MODE_BAK = "BAK" // backup
)

const (
	FOR_UPDATE = "FOR_UPDATE"
)

func BuildInstances(ctx context.Context) context.Context {
	var err error

	gameConf := ctx.Value("gameConf").(*gameConf.GameConfig)

	// mapは初期化されないので注意
	var dbShardWMap = map[int]*xorm.Engine{}

	master_dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		gameConf.Db.User,
		gameConf.Db.Pass,
		gameConf.Server.Host,
		gameConf.Server.Port,
		"game_master")

	// master_master
	dbMasterW, err := xorm.NewEngine("mysql", master_dsn)
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

	dbMasterR, err := xorm.NewEngine("mysql", shard_dsn)
	checkErr(err, "slaveDB master instance failed!!")

	// slave_shard
	var dbShardRMaps []map[int]*xorm.Engine
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

	// contextに設定
	ctx = context.WithValue(ctx, "dbMasterW", dbMasterW)
	ctx = context.WithValue(ctx, "dbMasterR", dbMasterR)
	ctx = context.WithValue(ctx, "dbShardWMap", dbShardWMap)
	ctx = context.WithValue(ctx, "dbShardRMaps", dbShardRMaps)

	return ctx
}

func StartTx(c *gin.Context) {
	gc := c.Value("globalContext").(context.Context)
	dbShardWMap := gc.Value("dbShardWMap").(map[int]*xorm.Engine)

	// すでに開始中の場合は何もしない
	iFace, valid := c.Get("txMap")
	if valid && iFace != nil {
		return
	}

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
	iFace, valid := c.Get("txMap")

	if valid && iFace != nil {
		txMap := iFace.(map[int]*xorm.Session)
		for _, v := range txMap {
			v.Rollback()
		}
		c.Set("txMap", nil)
	}
	// errを返す
}

// TODO:不要かも知れない
func Close(c *gin.Context) {
	// NOTE:txMapはキーが存在しているため、trueになる
	iFace, _ := c.Get("txMap")

	if iFace != nil {
		txMap := iFace.(map[int]*xorm.Session)
		for _, v := range txMap {
			v.Close()
		}
		c.Set("txMap", nil)
	}
}

func GetDBConnection(c *gin.Context, tableName string, options ...interface{}) (*xorm.Engine, error) {
	var err error
	var conn *xorm.Engine

	mode, _, err := optionCheck(options...)
	if err != nil {
		return conn, err
	}

	// db_conf_tableからshardかmasterを取得
	dbType := SHARD // shard

	// masterの場合
	switch dbType {
	case MASTER:
		gc := c.Value("globalContext").(context.Context)
		conn = gc.Value("dbMaster" + mode).(*xorm.Engine)
	case SHARD:
		conn, err = getDBShardConnection(c, mode)

	default:
		err = errors.New("undefined db type!!")
	}

	// shardの場合
	if conn == nil {
		err = errors.New("not found db connection!!")
	}
	return conn, err
}

func getDBShardConnection(c *gin.Context, mode string) (*xorm.Engine, error) {
	var err error

	var conn *xorm.Engine
	gc := c.Value("globalContext").(context.Context)

	// TODO:仮
	shardId := 1

	switch mode {
	case MODE_W:
		dbShardWMap := gc.Value("dbShardWMap").(map[int]*xorm.Engine)
		conn = dbShardWMap[shardId]

	case MODE_R:
		slaveIndex := c.Value("slaveIndex").(int)
		dbShardRMaps := gc.Value("dbShardRMaps").([]map[int]*xorm.Engine)
		shardMap := dbShardRMaps[slaveIndex]

		conn = shardMap[shardId]

	case MODE_BAK:
		// TODO:実装

	default:
		err = errors.New("invalid mode!!")
	}
	return conn, err
}

func GetDBSession(c *gin.Context) (*xorm.Session, error) {

	// TODO:仮
	shardId := 1

	var err error
	var tx *xorm.Session

	// セッションを開始してない場合はエラーとしておく
	iFace, valid := c.Get("txMap")
	if valid {
		sMap := iFace.(map[int]*xorm.Session)
		tx = sMap[shardId]
	} else {
		err = errors.New("transaction not found!!")
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

func optionCheck(options ...interface{}) (string, bool, error) {
	var err error

	var mode = MODE_R
	var isForUpdate bool

	for _, v := range options {

		switch v.(type) {
		case string:
			str := v.(string)
			if str == MODE_W || str == MODE_R || str == MODE_BAK {
				mode = str
			} else if str == FOR_UPDATE {
				isForUpdate = true
			} else {
				err = errors.New("unknown option!!")
				break
			}

		default:
			err = errors.New("can not check this type!!")
			log.Error(v)
			break
		}
	}
	log.Info(mode)
	log.Info(isForUpdate)
	return mode, isForUpdate, err
}
