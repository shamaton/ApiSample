package db

/**************************************************************************************************/
/*!
 *  db.go
 *
 *  DBのhandle/transaction管理
 *
 */
/**************************************************************************************************/
import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"

	"golang.org/x/net/context"
	"gopkg.in/gorp.v1"

	"sample/common/err"
	"sample/common/log"
	. "sample/conf"
	ckey "sample/conf/context"
	"sample/conf/gameConf"
)

var (
	slaveWeights []int
	shardIds     []int
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
func BuildInstances(ctx context.Context) (context.Context, err.ErrWriter) {
	gc := ctx.Value(ckey.GameConfig).(*gameConf.GameConfig)

	// make shards
	for i := 0; i < gc.Db.Shard; i++ {
		shardIds = append(shardIds, i+1)
	}

	// master - master
	masterW, ew := getWriteMaster(gc)
	ctx = context.WithValue(ctx, ckey.DbMasterW, masterW)
	if ew.HasErr() {
		return ctx, ew.Write()
	}

	// master - shard
	shardWMap, ew := getWriteShard(gc)
	ctx = context.WithValue(ctx, ckey.DbShardWMap, shardWMap)
	if ew.HasErr() {
		return ctx, ew.Write()
	}

	// slave master
	masterRs, ew := getReadOnlyMaster(gc)
	ctx = context.WithValue(ctx, ckey.DbMasterRs, masterRs)
	if ew.HasErr() {
		return ctx, ew.Write()
	}

	// slave shard
	shardRMaps, ew := getReadOnlyShard(gc)
	ctx = context.WithValue(ctx, ckey.DbShardRMaps, shardRMaps)
	if ew.HasErr() {
		return ctx, ew.Write()
	}

	// slaveの選択比重
	for slave_index, slaveConf := range gc.Server.Slave {
		for i := 0; i < slaveConf.Weight; i++ {
			slaveWeights = append(slaveWeights, slave_index)
		}
	}

	// TODO:BAK MODE

	return ctx, ew
}

/**************************************************************************************************/
/*!
 *  書き込み可能マスタDBへの接続
 *
 *  \param  gc : game config
 *  \return  DbMap, エラー
 */
/**************************************************************************************************/
func getWriteMaster(gc *gameConf.GameConfig) (*gorp.DbMap, err.ErrWriter) {
	dbMap, ew := getDbMap(gc.Db, gc.Server.Host, gc.Server.Port, "game_master")
	if ew.HasErr() {
		log.Critical("master : game_master setup failed!!")
		return nil, ew
	}
	return dbMap, err.NewErrWriter()
}

/**************************************************************************************************/
/*!
 *  書き込み可能シャードDBへの接続
 *
 *  \param  gc : game config
 *  \return  map[shard_id]DbMap, エラー
 */
/**************************************************************************************************/
func getWriteShard(gc *gameConf.GameConfig) (map[int]*gorp.DbMap, err.ErrWriter) {
	ew := err.NewErrWriter()
	var shardMap = map[int]*gorp.DbMap{}

	for _, shardId := range shardIds {
		// database
		dbName := "game_shard_" + strconv.Itoa(shardId)

		// mapping
		shardMap[shardId], ew = getDbMap(gc.Db, gc.Server.Host, gc.Server.Port, dbName)

		// error
		if ew.HasErr() {
			// すでに成功しているものをクローズする
			for _, dbMap := range shardMap {
				dbMap.Db.Close()
			}

			return nil, ew.Write("master : " + dbName + " setup failed!!")
		}
	}
	return shardMap, ew
}

/**************************************************************************************************/
/*!
 *  読み取り専用マスタDBへの接続
 *
 *  \param  gc : game config
 *  \return  [slave_index]DbMap, エラー
 */
/**************************************************************************************************/
func getReadOnlyMaster(gc *gameConf.GameConfig) ([]*gorp.DbMap, err.ErrWriter) {

	var masterRs []*gorp.DbMap

	// エラー時
	errorFunc := func(dbs []*gorp.DbMap) {
		for _, db := range dbs {
			db.Db.Close()
		}
	}

	for _, slaveConf := range gc.Server.Slave {
		// mapping
		masterR, ew := getDbMap(gc.Db, slaveConf.Host, slaveConf.Port, "game_master")

		// error
		if ew.HasErr() {
			errorFunc(masterRs)
			return nil, ew.Write("slave : game_master setup failed!!")
		}

		// SET READ ONLY
		_, e := masterR.Exec("SET TRANSACTION READ ONLY")
		if e != nil {
			log.Critical()
			errorFunc(masterRs)
			return nil, ew.Write("slave : game_master transaction setting failed!!", e)
		}

		// add slave masters
		masterRs = append(masterRs, masterR)
	}
	return masterRs, err.NewErrWriter()
}

/**************************************************************************************************/
/*!
 *  読み取り専用シャードDBへの接続
 *
 *  \param  gc : game config
 *  \return  [slave_index]map[shard_id]DbMap, エラー
 */
/**************************************************************************************************/
func getReadOnlyShard(gc *gameConf.GameConfig) ([]map[int]*gorp.DbMap, err.ErrWriter) {

	var shardMaps []map[int]*gorp.DbMap

	// エラー時
	errorFunc := func(dbMaps []map[int]*gorp.DbMap) {
		for _, dbMap := range dbMaps {
			for _, db := range dbMap {
				db.Db.Close()
			}
		}
	}

	for _, slaveConf := range gc.Server.Slave {

		var shardMap = map[int]*gorp.DbMap{}

		for _, shardId := range shardIds {
			// database
			dbName := "game_shard_" + strconv.Itoa(shardId)

			// mapping
			db, ew := getDbMap(gc.Db, slaveConf.Host, slaveConf.Port, dbName)

			// error
			if ew.HasErr() {
				errorFunc(shardMaps)
				return nil, ew.Write("slave : " + dbName + " setup failed!!")
			}

			_, e := db.Exec("SET TRANSACTION READ ONLY")
			if e != nil {
				errorFunc(shardMaps)
				return nil, ew.Write("slave : "+dbName+" transaction setting failed!!", e)
			}
			shardMap[shardId] = db
		}
		shardMaps = append(shardMaps, shardMap)

	}
	return shardMaps, err.NewErrWriter()
}

/**************************************************************************************************/
/*!
 *  DBマップを取得する
 *
 *  \return  使用するslaveのindex
 */
/**************************************************************************************************/
func getDbMap(dbConf gameConf.DbConfig, host, port, dbName string) (*gorp.DbMap, err.ErrWriter) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local", dbConf.User, dbConf.Pass, host, port, dbName)

	db, e := sql.Open("mysql", dsn)
	if e != nil {
		return nil, err.NewErrWriter(e)
	}

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}
	return dbmap, err.NewErrWriter()
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

/**************************************************************************************************/
/*!
 *  シャードIDを返す
 *
 *  \return  使用するslaveのindex
 */
/**************************************************************************************************/
func GetShardIds() []int {
	return shardIds
}

/**************************************************************************************************/
/*!
 *  クローズ処理
 *
 *  失敗しても処理を続ける
 *
 *  \return  なし
 */
/**************************************************************************************************/
func Close(ctx context.Context) {

	// write master
	masterW, ok := ctx.Value(ckey.DbMasterW).(*gorp.DbMap)
	if ok {
		masterW.Db.Close()
	}

	// write shard
	shardMap, ok := ctx.Value(ckey.DbShardWMap).(map[int]*gorp.DbMap)
	if ok {
		for _, dbMap := range shardMap {
			dbMap.Db.Close()
		}
	}

	// read master
	masterRs, ok := ctx.Value(ckey.DbMasterRs).([]*gorp.DbMap)
	if ok {
		for _, masterR := range masterRs {
			masterR.Db.Close()
		}
	}

	// read shard
	dbShardRMaps, ok := ctx.Value(ckey.DbShardRMaps).([]map[int]*gorp.DbMap)
	if ok {
		for _, dbShardRMap := range dbShardRMaps {
			for _, dbShardR := range dbShardRMap {
				dbShardR.Db.Close()
			}
		}
	}
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
func MasterTxStart(c *gin.Context, mode string) err.ErrWriter {

	isKey, txKey := ckey.IsMasterRTxStart, ckey.TxMasterR
	if mode == MODE_W {
		isKey, txKey = ckey.IsMasterWTxStart, ckey.TxMasterW
	}

	// すでに開始中の場合は何もしない
	if isTransactonStart(c, isKey) {
		return err.NewErrWriter()
	}

	// dbハンドル取得
	db, ew := GetDBMasterConnection(c, mode)
	if ew.HasErr() {
		return ew.Write()
	}

	// transaction start
	tx, err := db.Begin()
	if err != nil {
		return ew.Write(err)
	}

	// リクエストコンテキストに保存
	c.Set(txKey, tx)
	c.Set(isKey, true)

	return ew
}

/**************************************************************************************************/
/*!
 *  すべてのshardでトランザクションを開始する
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func ShardAllTxStart(c *gin.Context, mode string) err.ErrWriter {

	isKey, txKey := ckey.IsShardRTxStart, ckey.TxShardRMap
	if mode == MODE_W {
		isKey, txKey = ckey.IsShardWTxStart, ckey.TxShardWMap
	}

	// すでに開始中の場合は何もしない
	if isTransactonStart(c, isKey) {
		return err.NewErrWriter()
	}

	// dbハンドルマップを取得
	dbMap, ew := GetDBShardMap(c, mode)
	if ew.HasErr() {
		return ew.Write()
	}

	var txMap = map[int]*gorp.Transaction{}
	// txのマップを作成
	for k, v := range dbMap {
		tx, e := v.Begin()

		// エラーが起きた時点でおかしいのでreturn
		if e != nil {
			return ew.Write(e)
		}
		txMap[k] = tx
	}

	// リクエストコンテキストに保存
	c.Set(txKey, txMap)
	c.Set(isKey, true)

	return ew
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
func Commit(c *gin.Context) err.ErrWriter {
	ew := masterCommit(c)
	ew = shardCommit(c)
	// slaveでcommitすることはないのでrollbackしておく
	ew = masterRollback(c, MODE_R)
	ew = shardRollback(c, MODE_R)
	return ew
}

/**************************************************************************************************/
/*!
 *  masterの開始したtransactionをcommitする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func masterCommit(c *gin.Context) err.ErrWriter {
	var e error
	iFace, valid := c.Get(ckey.TxMasterW)

	if valid && iFace != nil {
		tx := iFace.(*gorp.Transaction)
		e = tx.Commit()

		// エラーじゃなければ削除
		if e == nil {
			c.Set(ckey.TxMasterW, nil)
			c.Set(ckey.IsMasterWTxStart, false)
		} else {
			return err.NewErrWriter(e)
		}
	}
	return err.NewErrWriter()
}

/**************************************************************************************************/
/*!
 *  shardの開始したtransactionをcommitする
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func shardCommit(c *gin.Context) err.ErrWriter {
	var e error
	var hasError = false

	iFace, valid := c.Get(ckey.TxShardWMap)

	if valid && iFace != nil {
		// 取得してすべてcommitする
		txMap := iFace.(map[int]*gorp.Transaction)
		for k, v := range txMap {
			e = v.Commit()
			// 正常な場合、削除する
			if e == nil {
				delete(txMap, k)
			} else {
				hasError = true
			}
		}

		// エラーが起きてなければ削除
		if !hasError {
			c.Set(ckey.TxShardWMap, nil)
			c.Set(ckey.IsShardWTxStart, false)
		} else {
			return err.NewErrWriter(e)
		}
	}
	return err.NewErrWriter()
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
func RollBack(c *gin.Context) err.ErrWriter {
	ew := masterRollback(c, MODE_W)
	ew = masterRollback(c, MODE_R)
	ew = shardRollback(c, MODE_W)
	ew = shardRollback(c, MODE_R)
	return ew
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
func masterRollback(c *gin.Context, mode string) err.ErrWriter {
	var e error

	isKey, txKey := ckey.IsMasterRTxStart, ckey.TxMasterR
	if mode == MODE_W {
		isKey, txKey = ckey.IsMasterWTxStart, ckey.TxMasterW
	}

	iFace, valid := c.Get(txKey)

	if valid && iFace != nil {
		tx := iFace.(*gorp.Transaction)
		e = tx.Rollback()

		// エラーじゃなければ削除
		if e == nil {
			c.Set(txKey, nil)
			c.Set(isKey, false)
		} else {
			return err.NewErrWriter(e)
		}
	}
	return err.NewErrWriter()
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
func shardRollback(c *gin.Context, mode string) err.ErrWriter {
	var e error
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
			e = v.Rollback()
			// 正常な場合、削除する
			if e == nil {
				delete(txMap, k)
			} else {
				hasError = true
			}
		}

		// エラーが起きてなければ削除
		if !hasError {
			c.Set(txKey, nil)
			c.Set(isKey, false)
		} else {
			return err.NewErrWriter(e)
		}
	}
	return err.NewErrWriter()
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
func GetTransaction(c *gin.Context, mode string, isShard bool, shardId int) (*gorp.Transaction, err.ErrWriter) {
	var tx *gorp.Transaction

	switch isShard {
	case true:
		// shard
		// トランザクションを開始してない場合、中で開始する
		ew := ShardAllTxStart(c, mode)

		if ew.HasErr() {
			return nil, ew.Write("shard transaction start failed!!")
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
		ew := MasterTxStart(c, mode)

		if ew.HasErr() {
			return nil, ew.Write("master transaction start failed!!")
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

	ew := err.NewErrWriter()
	if tx == nil {
		return nil, ew.Write("not found transaction!!")
	}

	return tx, ew
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
func GetDBConnection(c *gin.Context, mode string, isShard bool, shardId int) (*gorp.DbMap, err.ErrWriter) {
	ew := err.NewErrWriter()
	var conn *gorp.DbMap

	switch isShard {
	case true:
		// shard
		conn, ew = GetDBShardConnection(c, mode, shardId)

	case false:
		// master
		conn, ew = GetDBMasterConnection(c, mode)

	default:
		// to do nothing
	}

	if conn == nil {
		return nil, ew.Write("not found db connection!!")
	}
	return conn, ew
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
func GetDBMasterConnection(c *gin.Context, mode string) (*gorp.DbMap, err.ErrWriter) {
	var conn *gorp.DbMap
	ew := err.NewErrWriter()

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
		return nil, ew.Write("invalid mode!!")
	}

	//
	if conn == nil {
		return nil, ew.Write("connection is nil!!")
	}

	return conn, ew
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
func GetDBShardConnection(c *gin.Context, mode string, shardId int) (*gorp.DbMap, err.ErrWriter) {
	var conn *gorp.DbMap

	shardMap, err := GetDBShardMap(c, mode)
	if err.Err() != nil {
		return nil, err.Write()
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
func GetDBShardMap(c *gin.Context, mode string) (map[int]*gorp.DbMap, err.ErrWriter) {
	ew := err.NewErrWriter()
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
		return nil, ew.Write("invalid mode!!")
	}

	// 存在確認
	if shardMap == nil {
		return nil, ew.Write("shardMap is nil!!")
	}

	return shardMap, ew
}
