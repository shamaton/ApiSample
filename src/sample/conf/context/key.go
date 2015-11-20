package context

/**
 * コンテキストで一意にするためのキー
 */
type ContextKey string

// TODO:リクエストコンテキストとグローバルコンテキストで分ける

const (
	// context
	GContext   string = "gContext"
	GameConfig        = "gameConfig"

	// memd
	MemdPool = "redis"

	// DB
	DbMasterW    = "dbMasterW"
	DbShardWMap  = "dbShardWMap"
	DbMasterRs   = "dbMasterRs"
	DbShardRMaps = "dbShardRMaps"
	TxMasterW    = "txMasterW"
	TxShardWMap  = "txShardWMap"
	TxMasterR    = "txMasterR"
	TxShardRMap  = "txShardRMap"

	IsMasterWTxStart = "isMasterWTxStart"
	IsShardWTxStart  = "isShardWTxStart"
	IsMasterRTxStart = "isMasterRTxStart"
	IsShardRTxStart  = "isShardRTxStart"

	SlaveIndex = "slaveIndex"
)

/**
 * リクエストコンテキスト
 */
const (
	RedisRConn     = "RedisRConn"
	RedisWconn     = "RedisWconn"
	IsRedisTxStart = "IsRedisTxStart"
)
