package model

import "log"

type modelBase struct {
	pks   []string
	shard bool
}

func (m *modelBase) SelectByPk() {
	log.Println("call select by pk")
}

/*
func ForUpdate
*/

/*
// shardを判断して返す, falseの場合masterをそのまま返す
func (m *modelBase) GetDBHandle(ctx context.Context, shardKey int, tableName string, mode string) *gorp.DbMap {
  // db table conf参照
  tableConf := ctx.Value("DB").(*gorp.DbMap)

  // type:userの場合shardのマスタから取得する
  shardId := 1//GetUserShard(shardKey) //tableConf.Select(&userShard, "select user_id, shard_id from user_shard", shardKey)

  // type:groupの場合は計算する
  shardId = shardKey % 4

  var connDb *gorp.DbMap

  switch mode {
  case MODE_W:

  }

	return connDb
}
*/

/*
func (m *modelBase) Select(db interface{}, holder interface{}, query string, args... interface{}) {
  // dbmap or tx
  switch dbi = db.(type) {
  case (*gorp.DbMap):

  }
}
*/
