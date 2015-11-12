package context

import (

)

/**
 * コンテキストで一意にするためのキー
 */
type ContextKey string

const (
	// context
	GContext ContextKey = "gContext"

	// memd
	MemdPool = "redis"

	// DB
	DbMasterW   = "dbMasterW"
	DbShardWMap = "dbShardWMap"
	DbMasterRs = "dbMasterRs"
	DbShardRMaps = "dbShardRMaps"
	TxMaster = "txMaster"
	TxShardMap = "txShardMap"

	IsMasterTxStart = "isMasterTxStart"
	IsShardTxStart = "isShardTxStart"

	SlaveIndex = "slaveIndex"
)