package context

/**
 * コンテキストで一意にするためのキー
 */
type ContextKey string

/**
 * グローバルコンテキスト
 */
const (
	GameConfig ContextKey = "gameConfig"

	// memd
	RedisPool = "RedisPool"
	RedisInit = "RedisInit"

	// DB
	DbMasterW    = "dbMasterW"
	DbShardWMap  = "dbShardWMap"
	DbMasterRs   = "dbMasterRs"
	DbShardRMaps = "dbShardRMaps"

	SlaveIndex = "slaveIndex"
)

/**
 * リクエストコンテキスト
 */
const (
	GContext string = "gContext"

	TxMasterW   = "txMasterW"
	TxShardWMap = "txShardWMap"
	TxMasterR   = "txMasterR"
	TxShardRMap = "txShardRMap"

	IsMasterWTxStart = "isMasterWTxStart"
	IsShardWTxStart  = "isShardWTxStart"
	IsMasterRTxStart = "isMasterRTxStart"
	IsShardRTxStart  = "isShardRTxStart"

	RedisRConn     = "RedisRConn"
	RedisWconn     = "RedisWconn"
	IsRedisTxStart = "IsRedisTxStart"
)
