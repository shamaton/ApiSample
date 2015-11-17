package DBI

/**************************************************************************************************/
/*!
 *  DBI.go
 *
 *  DBのhandle/transaction管理
 *
 */
/**************************************************************************************************/
import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"

	"golang.org/x/net/context"
	"gopkg.in/gorp.v1"

	ckey "sample/conf/context"
	"sample/conf/gameConf"
)

var (
	slaveWeights []int
	shardIds     []int
)

/**
 * DB MODE
 */
const (
	MODE_W   = "W"   // master
	MODE_R   = "R"   // slave
	MODE_BAK = "BAK" // backup
)

/**************************************************************************************************/
/*!
 *  dbハンドラを生成する
 *
 *  masterは1つのハンドラをもち、slaveは複数のハンドラを持つ
 *  master
 *   master *db
 *   shard map[int]*db
 * ----------------
 *  slave
 *   master []*db
 *   shard []map[int]*db
 *
 *
 *  \param   ctx : グローバルなコンテキスト
 *  \return  ハンドラ登録済みのコンテキスト、エラー
 */
/**************************************************************************************************/
func BuildInstances(ctx context.Context) (context.Context, error) {
	var err error

	gc := ctx.Value("gameConf").(*gameConf.GameConfig)

	// gorpのオブジェクトを取得
	getGorp := func(dbConf gameConf.DbConfig, host, port, dbName string) (*gorp.DbMap, error) {

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local", dbConf.User, dbConf.Pass, host, port, dbName)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Critical(err)
		}

		// construct a gorp DbMap
		dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
		return dbmap, err
	}

	// make shards
	for i := 0; i < gc.Db.Shard; i++ {
		shardIds = append(shardIds, i+1)
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

		_, err = masterR.Exec("SET TRANSACTION READ ONLY")
		if err != nil {
			log.Critical("slave : game_master transaction setting failed!!")
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
			db, err := getGorp(
				gc.Db,
				slaveConf.Host,
				slaveConf.Port,
				dbName)

			// error
			if err != nil {
				log.Critical("slave : " + dbName + " setup failed!!")
				return ctx, err
			}

			_, err = db.Exec("SET TRANSACTION READ ONLY")
			if err != nil {
				log.Critical("slave : " + dbName + " transaction setting failed!!")
				return ctx, err
			}
			shardMap[shardId] = db
		}
		shardRMaps = append(shardRMaps, shardMap)

		// slaveの選択比重
		for i := 0; i < slaveConf.Weight; i++ {
			slaveWeights = append(slaveWeights, slave_index)
		}
	}

	// contextに設定
	ctx = context.WithValue(ctx, ckey.DbMasterW, masterW)
	ctx = context.WithValue(ctx, ckey.DbShardWMap, shardWMap)

	ctx = context.WithValue(ctx, ckey.DbMasterRs, masterRs)
	ctx = context.WithValue(ctx, ckey.DbShardRMaps, shardRMaps)

	// TODO:BAK MODE

	return ctx, err
}

/**************************************************************************************************/
/*!
 *  リクエスト中に使用するslaveを決める
 *
 *  \return  使用するslaveのindex
 */
/**************************************************************************************************/
func DecideUseSlave() int {
	slaveIndex := rand.Intn(len(slaveWeights))
	return slaveWeights[slaveIndex]
}

/**
 * BEGIN function
 */
/**************************************************************************************************/
/*!
 *  masterでトランザクションを開始する
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func MasterTxStart(c *gin.Context, mode string) error {
	var err error

	isKey, txKey := ckey.IsMasterRTxStart, ckey.TxMasterR
	if mode == MODE_W {
		isKey, txKey = ckey.IsMasterWTxStart, ckey.TxMasterW
	}

	// すでに開始中の場合は何もしない
	if isTransactonStart(c, isKey) {
		return nil
	}

	// dbハンドル取得
	db, err := GetDBMasterConnection(c, mode)
	if err != nil {
		return err
	}

	// transaction start
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// リクエストコンテキストに保存
	c.Set(txKey, tx)
	c.Set(isKey, true)

	return err
}

/**************************************************************************************************/
/*!
 *  すべてのshardでトランザクションを開始する
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func ShardAllTxStart(c *gin.Context, mode string) error {
	var err error

	isKey, txKey := ckey.IsShardRTxStart, ckey.TxShardRMap
	if mode == MODE_W {
		isKey, txKey = ckey.IsShardWTxStart, ckey.TxShardWMap
	}

	// すでに開始中の場合は何もしない
	if isTransactonStart(c, isKey) {
		return nil
	}

	// dbハンドルマップを取得
	dbMap, err := GetDBShardMap(c, mode)
	if err != nil {
		return err
	}

	var txMap = map[int]*gorp.Transaction{}
	// txのマップを作成
	for k, v := range dbMap {
		tx, err := v.Begin()

		// エラーが起きた時点でおかしいのでreturn
		if err != nil {
			return err
		}
		txMap[k] = tx
	}

	// リクエストコンテキストに保存
	c.Set(txKey, txMap)
	c.Set(isKey, true)

	return err
}

/**
 * COMMIT function
 */
/**************************************************************************************************/
/*!
 *  開始した全てのtransactionをcommitする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func Commit(c *gin.Context) error {
	err := masterCommit(c)
	err = shardCommit(c)
	// slaveでcommitすることはないのでrollbackしておく
	err = masterRollback(c, MODE_R)
	err = shardRollback(c, MODE_R)
	return err
}

/**************************************************************************************************/
/*!
 *  masterの開始したtransactionをcommitする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func masterCommit(c *gin.Context) error {
	var err error
	iFace, valid := c.Get(ckey.TxMasterW)

	if valid && iFace != nil {
		tx := iFace.(*gorp.Transaction)
		err = tx.Commit()

		// エラーじゃなければ削除
		if err == nil {
			c.Set(ckey.TxMasterW, nil)
			c.Set(ckey.IsMasterWTxStart, false)
		}
	}
	return err
}

/**************************************************************************************************/
/*!
 *  shardの開始したtransactionをcommitする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func shardCommit(c *gin.Context) error {
	var err error
	var hasError = false

	iFace, valid := c.Get(ckey.TxShardWMap)

	if valid && iFace != nil {
		// 取得してすべてcommitする
		txMap := iFace.(map[int]*gorp.Transaction)
		for k, v := range txMap {
			err = v.Commit()
			// 正常な場合、削除する
			if err == nil {
				delete(txMap, k)
			}
		}

		// エラーが起きてなければ削除
		if !hasError {
			c.Set(ckey.TxShardWMap, nil)
			c.Set(ckey.IsShardWTxStart, false)
		}
	}
	return err
}

/**
 * ROLLBACK function
 */
/**************************************************************************************************/
/*!
 *  開始した全てのtransactionをrollbackする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func RollBack(c *gin.Context) error {
	err := masterRollback(c, MODE_W)
	err = masterRollback(c, MODE_R)
	err = shardRollback(c, MODE_W)
	err = shardRollback(c, MODE_R)
	return err
}

/**************************************************************************************************/
/*!
 *  masterの開始したtransactionをrollbackする
 *
 *  \param   c    : コンテキスト
 *  \param   mode : モード
 *  \return  エラー
 */
/**************************************************************************************************/
func masterRollback(c *gin.Context, mode string) error {
	var err error

	isKey, txKey := ckey.IsMasterRTxStart, ckey.TxMasterR
	if mode == MODE_W {
		isKey, txKey = ckey.IsMasterWTxStart, ckey.TxMasterW
	}

	iFace, valid := c.Get(txKey)

	if valid && iFace != nil {
		tx := iFace.(*gorp.Transaction)
		err = tx.Rollback()

		// エラーじゃなければ削除
		if err == nil {
			c.Set(txKey, nil)
			c.Set(isKey, false)
		}
	}
	return err
}

/**************************************************************************************************/
/*!
 *  shardの開始したtransactionをrollbackする
 *
 *  \param   c    : コンテキスト
 *  \param   mode : モード
 *  \return  エラー
 */
/**************************************************************************************************/
func shardRollback(c *gin.Context, mode string) error {
	var err error
	var hasError = false

	isKey, txKey := ckey.IsShardRTxStart, ckey.TxShardRMap
	if mode == MODE_W {
		isKey, txKey = ckey.IsShardWTxStart, ckey.TxShardWMap
	}

	iFace, valid := c.Get(txKey)

	if valid && iFace != nil {
		// 取得してすべてrollbackする
		txMap := iFace.(map[int]*gorp.Transaction)
		for k, v := range txMap {
			err = v.Rollback()
			// 正常な場合、削除する
			if err == nil {
				delete(txMap, k)
			}
		}

		// エラーが起きてなければ削除
		if !hasError {
			c.Set(txKey, nil)
			c.Set(isKey, false)
		}
	}
	return err
}

/**
 * get transaction function
 */
/**************************************************************************************************/
/*!
 *  トランザクションを取得する(開始してない場合、開始する)
 *
 *  \param   c       : コンテキスト
 *  \param   isShard : trueの場合shardのDBハンドルを取得する
 *  \param   shardId : 存在するshard ID
 *  \return  トランザクション、エラー
 */
/**************************************************************************************************/
func GetTransaction(c *gin.Context, mode string, isShard bool, shardId int) (*gorp.Transaction, error) {
	var err error
	var tx *gorp.Transaction

	switch isShard {
	case true:
		// shard
		// トランザクションを開始してない場合、中で開始する
		err = ShardAllTxStart(c, mode)

		if err != nil {
			log.Error("shard transaction start failed!!")
			return tx, err
		}

		// context key
		txKey := ckey.TxShardRMap
		if mode == MODE_W {
			txKey = ckey.TxShardWMap
		}

		// get
		iFace, valid := c.Get(txKey)
		if valid && iFace != nil {
			sMap := iFace.(map[int]*gorp.Transaction)
			tx = sMap[shardId]
		}

	case false:
		// master
		// トランザクションを開始してない場合、開始する
		err = MasterTxStart(c, mode)

		if err != nil {
			log.Error("master transaction start failed!!")
			return tx, err
		}

		// context key
		txKey := ckey.TxMasterR
		if mode == MODE_W {
			txKey = ckey.TxMasterW
		}

		// get
		iFace, valid := c.Get(txKey)
		if valid && iFace != nil {
			tx = iFace.(*gorp.Transaction)
		}

	default:
		// to do nothing
	}

	if tx == nil {
		err = errors.New("not found transaction!!")
		log.Error(err)
	}

	return tx, err
}

/**************************************************************************************************/
/*!
 *  トランザクションを開始したか確認する
 *
 *  \param   c      : コンテキスト
 *  \param   key    : コンテキスト内キー
 *  \return  true/false
 */
/**************************************************************************************************/
func isTransactonStart(c *gin.Context, key string) bool {
	iFace, valid := c.Get(key)
	if valid && iFace != nil {
		return iFace.(bool)
	}
	return false
}

/**
 * get db connection function
 */
/**************************************************************************************************/
/*!
 *  各DBへのハンドルを取得する
 *
 *  \param   c       : コンテキスト
 *  \param   mode    : W, R, BAK
 *  \param   isShard : trueの場合shardのDBハンドルを取得する
 *  \param   shardId : 存在するshard ID
 *  \return  DBハンドル、エラー
 */
/**************************************************************************************************/
func GetDBConnection(c *gin.Context, mode string, isShard bool, shardId int) (*gorp.DbMap, error) {
	var err error
	var conn *gorp.DbMap

	switch isShard {
	case true:
		// shard
		conn, err = GetDBShardConnection(c, mode, shardId)

	case false:
		// master
		conn, err = GetDBMasterConnection(c, mode)

	default:
		// to do nothing
	}

	if conn == nil {
		err = errors.New("not found db connection!!")
	}
	return conn, err
}

/**************************************************************************************************/
/*!
 *  masterのDBハンドルを取得する
 *
 *  \param   c : コンテキスト
 *  \param   mode : W, R, BAK
 *  \return  DBハンドル、エラー
 */
/**************************************************************************************************/
func GetDBMasterConnection(c *gin.Context, mode string) (*gorp.DbMap, error) {
	var conn *gorp.DbMap
	var err error

	gc := c.Value(ckey.GContext).(context.Context)

	switch mode {
	case MODE_W:
		conn = gc.Value(ckey.DbMasterW).(*gorp.DbMap)

	case MODE_R:
		slaveIndex := c.Value(ckey.SlaveIndex).(int)
		masterRs := gc.Value(ckey.DbMasterRs).([]*gorp.DbMap)
		conn = masterRs[slaveIndex]

	case MODE_BAK:
	// TODO:実装

	default:
		err = errors.New("invalid mode!!")
	}

	//
	if conn == nil {
		err = errors.New("connection is nil!!")
	}

	return conn, err
}

/**************************************************************************************************/
/*!
 *  指定したShardIDのハンドルを取得する
 *
 *  \param   c : コンテキスト
 *  \param   mode : W, R, BAK
 *  \param   shardId : shard ID
 *  \return  DBハンドル、エラー
 */
/**************************************************************************************************/
func GetDBShardConnection(c *gin.Context, mode string, shardId int) (*gorp.DbMap, error) {
	var conn *gorp.DbMap
	var err error

	shardMap, err := GetDBShardMap(c, mode)
	if err != nil {
		return nil, err
	}
	conn = shardMap[shardId]

	return conn, err
}

/**************************************************************************************************/
/*!
 *  ShardのDBハンドルマップを取得する
 *
 *  \param   c : コンテキスト
 *  \param   mode : W, R, BAK
 *  \return  DBハンドルマップ、エラー
 */
/**************************************************************************************************/
func GetDBShardMap(c *gin.Context, mode string) (map[int]*gorp.DbMap, error) {
	var err error
	var shardMap map[int]*gorp.DbMap

	gc := c.Value(ckey.GContext).(context.Context)

	switch mode {
	case MODE_W:
		shardMap = gc.Value(ckey.DbShardWMap).(map[int]*gorp.DbMap)

	case MODE_R:
		slaveIndex := c.Value(ckey.SlaveIndex).(int)
		dbShardRMaps := gc.Value(ckey.DbShardRMaps).([]map[int]*gorp.DbMap)
		shardMap = dbShardRMaps[slaveIndex]

	case MODE_BAK:
	// TODO:実装

	default:
		err = errors.New("invalid mode!!")
	}

	// 存在確認
	if shardMap == nil {
		err = errors.New("shardMap is nil!!")
	}

	return shardMap, err
}
