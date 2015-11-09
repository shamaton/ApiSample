package DBI

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/context"
	"math/rand"
	"sample/conf/gameConf"
	"strconv"

	"database/sql"
	"errors"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"gopkg.in/gorp.v1"
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

// masterは1つのハンドラをもち、slaveは複数のハンドラを持つ
// master
//  master *db
//  shard map[int]*db
// ----------------
// slave
//  master []*db
//  shard []map[int]*db
func BuildInstances(ctx context.Context) (context.Context, error) {
	var err error

	gc := ctx.Value("gameConf").(*gameConf.GameConfig)

	// gorpのオブジェクトを取得
	getGorp := func(dbConf gameConf.DbConfig, host, port, dbName string) (*gorp.DbMap, error) {

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", dbConf.User, dbConf.Pass, host, port, dbName)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Critical(err)
		}

		// construct a gorp DbMap
		dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
		return dbmap, err
	}

	// master - master
	masterW, err := getGorp(gc.Db, gc.Server.Host, gc.Server.Port, "game_master")
	if err != nil {
		log.Critical("master : game_master setup failed!!")
		return ctx, err
	}

	// master - shard
	var shardWMap = map[int]*gorp.DbMap{}
	for _, shardId := range shardIds {
		// database
		dbName := "game_shard_" + strconv.Itoa(shardId)

		// mapping
		shardWMap[shardId], err = getGorp(
			gc.Db,
			gc.Server.Host,
			gc.Server.Port,
			dbName)

		// error
		if err != nil {
			log.Critical("master : " + dbName + " setup failed!!")
			return ctx, err
		}
	}

	// read-only database
	// slave
	var masterRs []*gorp.DbMap
	var shardRMaps []map[int]*gorp.DbMap
	for slave_index, slaveConf := range gc.Server.Slave {
		///////////////////////////////////
		// MASTER
		// mapping
		masterR, err := getGorp(
			gc.Db,
			slaveConf.Host,
			slaveConf.Port,
			"game_master")

		// error
		if err != nil {
			log.Critical("slave : game_master setup failed!!")
			return ctx, err
		}

		// add slave masters
		masterRs = append(masterRs, masterR)

		///////////////////////////////////
		// SHARD
		var shardMap = map[int]*gorp.DbMap{}

		for _, shardId := range shardIds {
			// database
			dbName := "game_shard_" + strconv.Itoa(shardId)

			// mapping
			shardMap[shardId], err = getGorp(
				gc.Db,
				slaveConf.Host,
				slaveConf.Port,
				dbName)

			// error
			if err != nil {
				log.Critical("slave : " + dbName + " setup failed!!")
				return ctx, err
			}
		}
		shardRMaps = append(shardRMaps, shardMap)

		// slaveの選択比重
		for i := 0; i < slaveConf.Weight; i++ {
			slaveWeights = append(slaveWeights, slave_index)
		}
	}

	// contextに設定
	ctx = context.WithValue(ctx, "dbMasterW", masterW)
	ctx = context.WithValue(ctx, "dbShardWMap", shardWMap)

	ctx = context.WithValue(ctx, "dbMasterRs", masterRs)
	ctx = context.WithValue(ctx, "dbShardRMaps", shardRMaps)

	// TODO:BAK MODE

	return ctx, err
}

func StartTx(c *gin.Context) {
	gc := c.Value("globalContext").(context.Context)
	dbShardWMap := gc.Value("dbShardWMap").(map[int]*gorp.DbMap)

	// すでに開始中の場合は何もしない
	iFace, valid := c.Get("txMap")
	if valid && iFace != nil {
		return
	}

	var txMap = map[int]*gorp.Transaction{}
	// txのマップを作成
	for k, v := range dbShardWMap {
		log.Info(k, " start tx!!")
		txMap[k], _ = v.Begin()
	}
	c.Set("txMap", txMap)
	// errを返す
}

func Commit(c *gin.Context) {
	txMap := c.Value("txMap").(map[int]*gorp.Transaction)
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
		txMap := iFace.(map[int]*gorp.Transaction)
		for _, v := range txMap {
			v.Rollback()
		}
		c.Set("txMap", nil)
	}
	// errを返す
}

func GetDBConnection(c *gin.Context, tableName string, options ...interface{}) (*gorp.DbMap, error) {
	var err error
	var conn *gorp.DbMap

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
		conn = gc.Value("dbMaster" + mode).(*gorp.DbMap)
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

func getDBMasterConnection(c *gin.Context, mode string) (*gorp.DbMap, error) {
	var conn *gorp.DbMap
	var err error

	gc := c.Value("globalContext").(context.Context)

	switch mode {
	case MODE_W:
		conn = gc.Value("dbMasterW").(*gorp.DbMap)

	case MODE_R:
		slaveIndex := c.Value("slaveIndex").(int)
		masterRs := gc.Value("dbMasterRs").([]*gorp.DbMap)
		conn = masterRs[slaveIndex]

	case MODE_BAK:
	// TODO:実装

	default:
		err = errors.New("invalid mode!!")
	}

	return conn, err
}

func getDBShardConnection(c *gin.Context, mode string) (*gorp.DbMap, error) {
	var conn *gorp.DbMap
	var err error

	// TODO:仮
	shardId := 1

	shardMap, err := getDBShardMap(c, mode)
	if err != nil {
		return nil, err
	}
	conn = shardMap[shardId]

	return conn, err
}

func getDBShardMap(c *gin.Context, mode string) (map[int]*gorp.DbMap, error) {
	var err error
	var shardMap map[int]*gorp.DbMap

	gc := c.Value("globalContext").(context.Context)

	switch mode {
	case MODE_W:
		shardMap = gc.Value("dbShardWMap").(map[int]*gorp.DbMap)

	case MODE_R:
		slaveIndex := c.Value("slaveIndex").(int)
		dbShardRMaps := gc.Value("dbShardRMaps").([]map[int]*gorp.DbMap)
		shardMap = dbShardRMaps[slaveIndex]

	case MODE_BAK:
	// TODO:実装

	default:
		err = errors.New("invalid mode!!")
	}
	return shardMap, err
}

func GetDBSession(c *gin.Context) (*gorp.Transaction, error) {

	// TODO:仮
	shardId := 1

	var err error
	var tx *gorp.Transaction

	// セッションを開始してない場合はエラーとしておく
	iFace, valid := c.Get("txMap")
	if valid {
		sMap := iFace.(map[int]*gorp.Transaction)
		tx = sMap[shardId]
	} else {
		err = errors.New("transaction not found!!")
	}

	return tx, err
}

type shardType int

const (
	USER shardType = iota
	GROUP
)

// table
type UserShard struct {
	Id      int `xorm:"pk"`
	ShardId int
}

// とりあえずshard_idを取得する
func GetShardId(c *gin.Context, st shardType, value int) (int, error) {
	var shardId int
	var err error

	switch st {
	case USER:
		// ハンドル取得
		conn, err := getDBMasterConnection(c, MODE_R)
		if err != nil {
			log.Error("not found master connection!!")
			break
		}

		// user_shardを検索
		us := UserShard{Id: value}
		_, err = conn.Get(&us)
		if err != nil {
			log.Info("not found user shard id")
			break
		}
		shardId = us.ShardId

	case GROUP:
		// TODO:実装
	default:
		err = errors.New("undefined shard type!!")
	}

	return shardId, err
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
