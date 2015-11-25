package logic

/**************************************************************************************************/
/*!
 *  redis.go
 *
 *  redisに関連する操作群
 *
 */
/**************************************************************************************************/
import (
	"reflect"
	ckey "sample/conf/context"

	"encoding/json"

	"time"

	"sample/common/err"

	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"
)

/**
 * オプション用マップ
 */
type RedisOption map[string]interface{}
type optionFunc func(RedisOption) ([]interface{}, err.ErrWriter)

/**
 * redis accessor
 */
type redisRepo struct {
}

/**************************************************************************************************/
/*!
 *  操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewRedisRepo() *redisRepo {
	return &redisRepo{}
}

/*
EX seconds -- Set the specified expire time, in seconds.
PX milliseconds -- Set the specified expire time, in milliseconds.
NX -- Only set the key if it does not already exist.
XX -- Only set the key if it already exist.
*/
/**************************************************************************************************/
/*!
 *  COMMAND : SET
 *
 *  \param   c       : コンテキスト
 *  \param   key     : キー
 *  \param   value   : 保存値
 *  \param   options : [EX|PX] [NX|XX]
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Set(c *gin.Context, key string, value interface{}, options ...interface{}) err.ErrWriter {

	// オプションチェック
	optArgs, ew := this.checkOption(this.checkSetOption, options)
	if ew.HasErr() {
		return ew.Write()
	}

	// ポインタの場合要素を参照する
	var refVal reflect.Value
	ref := reflect.ValueOf(value)
	if ref.Kind() == reflect.Ptr {
		refVal = ref.Elem()
	} else {
		refVal = ref
	}

	// 実行に使う定義
	var args []interface{}

	// 構造体の場合、JSON化してSET
	switch refVal.Kind() {
	case reflect.Struct:
		j, e := json.Marshal(value)
		if e != nil {
			return ew.Write(e)
		}
		args = append(args, key, j)

	default:
		args = append(args, key, refVal)
	}

	// optionを後ろにつける
	args = append(args, optArgs...)

	// SET
	ew = this.send(c, "SET", args...)
	if ew.HasErr() {
		return ew.Write()
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  SETコマンドのoptionチェック
 *
 *  \param   options : [EX|PX] [NX|XX]
 *  \return  オプション配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) checkSetOption(option RedisOption) ([]interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()
	var args []interface{}
	setNxXx := 0
	setExPx := 0

	for key, value := range option {
		switch key {
		case "EX", "PX":
			args = append([]interface{}{key, value}, args...)
			setExPx++
		case "NX", "XX":
			args = append(args, key)
			setNxXx++
		default:
			return nil, ew.Write("invalid key : " + key)
		}
	}

	// 2重渡しチェック
	if setExPx > 1 || setNxXx > 1 {
		return nil, ew.Write("invalid option setting!!")
	}

	return args, ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : GET
 *
 *  \param   c       : コンテキスト
 *  \param   key     : キー
 *  \param   holder  : 格納先
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Get(c *gin.Context, key string, holder interface{}) err.ErrWriter {
	conn := this.getReadConnection(c)
	ew := err.NewErrWriter()

	ref := reflect.ValueOf(holder).Elem()

	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, e := redis.Int64(conn.Do("GET", key))
		if e != nil {
			return ew.Write(e)
		}
		ref.SetInt(v)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, e := redis.Uint64(conn.Do("GET", key))
		if e != nil {
			return ew.Write(e)
		}
		ref.SetUint(v)

	case reflect.String:
		v, e := redis.String(conn.Do("GET", key))
		if e != nil {
			return ew.Write(e)
		}
		ref.SetString(v)

	case reflect.Struct:
		v, e := redis.Bytes(conn.Do("GET", key))
		if e != nil {
			return ew.Write(e)
		}
		json.Unmarshal(v, holder)
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : EXISTS
 *
 *  \param   c    : コンテキスト
 *  \param   key  : キー
 *  \param   keys : 複数のキー
 *  \return  全て存在する場合true、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Exists(c *gin.Context, key string, keys ...string) (bool, err.ErrWriter) {
	conn := this.getReadConnection(c)
	ew := err.NewErrWriter()

	// keys...で渡せないのでinterfaceに入れなおす
	var is []interface{}
	is = append(is, key)
	for _, k := range keys {
		is = append(is, k)
	}

	// exec redis
	v, e := redis.Int(conn.Do("EXISTS", is...))
	if e != nil {
		return false, ew.Write(e)
	}

	// 返り値と比較
	var isExists bool
	if v == len(is) {
		isExists = true
	}
	return isExists, ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : EXPIRE
 *
 *  \param   c       : コンテキスト
 *  \param   key     : キー
 *  \param   second  : 秒
 *  \return  成功時true、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Expire(c *gin.Context, key string, second int) err.ErrWriter {

	ew := this.send(c, "EXPIRE", key, second)

	if ew.HasErr() {
		return ew.Write()
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : EXPIREAT
 *
 *  \param   c    : コンテキスト
 *  \param   key  : キー
 *  \param   time : timeオブジェクト
 *  \return  成功時true、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ExpireAt(c *gin.Context, key string, t time.Time) err.ErrWriter {

	ew := this.send(c, "EXPIREAT", key, t.Unix())

	if ew.HasErr() {
		return ew
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : ZADD
 *
 *  ex : ZADD, [option], ranking, 1(score), user(member)
 *
 *  \param   c       : コンテキスト
 *  \param   key     : キー
 *  \param   member  : メンバー値
 *  \param   score   : スコア
 *  \param   options : [NX|XX] [CH] [INCR]
 *  \return  失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZAdd(c *gin.Context, key string, member string, score int, options ...interface{}) err.ErrWriter {

	// オプションチェック
	optArgs, ew := this.checkOption(this.checkZAddOption, options)
	if ew.HasErr() {
		return ew.Write()
	}

	args := []interface{}{key}
	args = append(args, optArgs...)
	args = append(args, score, member)

	ew = this.send(c, "ZADD", args...)
	if ew.HasErr() {
		return ew.Write()
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  ZADDをまとめて実行する
 *
 *  ex : ZADD, [option], ranking, 1(score), user(member), score, member...
 *
 *  \param   c        : コンテキスト
 *  \param   key      : キー
 *  \param   scoreMap : [member:score]なマップ
 *  \param   options  : [NX|XX] [CH] [INCR]
 *  \return  addされた数、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZAdds(c *gin.Context, key string, scoreMap map[string]int, options ...interface{}) err.ErrWriter {

	// オプションチェック
	optArgs, ew := this.checkOption(this.checkZAddOption, options)
	if ew.HasErr() {
		return ew.Write()
	}

	var args []interface{}
	args = append(args, key)

	// optionを付加
	args = append(args, optArgs...)

	for member, score := range scoreMap {
		args = append(args, score, member)
	}

	ew = this.send(c, "ZADD", args...)
	if ew.HasErr() {
		return ew.Write()
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  ZADDのオプションチェック
 *
 *  \param   option : [NX|XX] [CH] [INCR]
 *  \return  オプション配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) checkZAddOption(option RedisOption) ([]interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()
	var args []interface{}
	setNxXx := 0

	keys := []string{"NX", "XX", "CH", "INCR"}

	for _, key := range keys {
		_, valid := option[key]
		if valid {
			args = append(args, key)
		}
	}

	// 2重渡しチェック
	if setNxXx > 1 {
		return nil, ew.Write("invalid option setting!!")
	}

	return args, ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : ZREVRANGE
 *
 *  ex : ZREVRANGE, ranking, start, stop
 *
 *  \param   c     : コンテキスト
 *  \param   key   : キー
 *  \param   start : 検索開始位置
 *  \param   stop  : 検索終了位置
 *  \return  [member:score]な配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZRevRange(c *gin.Context, key string, start int, stop int) ([]map[string]int, err.ErrWriter) {
	ew := err.NewErrWriter()
	conn := this.getReadConnection(c)

	values, e := redis.Values(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	if e != nil {
		return nil, ew.Write(e)
	}

	// mapping
	mapArray := []map[string]int{}
	for i := 0; i < len(values); i += 2 {
		m := map[string]int{}
		str, e := redis.String(values[i], nil)
		if e != nil {
			return nil, ew.Write(e)
		}
		value, e := redis.Int(values[i+1], nil)
		if e != nil {
			return nil, ew.Write(e)
		}
		m[str] = value
		mapArray = append(mapArray, m)
	}

	return mapArray, ew
}

/**************************************************************************************************/
/*!
 *  ZREVRANGEの全範囲版
 *
 *  ex : ZREVRANGE, ranking, 0, -1
 *
 *  \param   c     : コンテキスト
 *  \param   key   : キー
 *  \param   start : 検索開始位置
 *  \param   stop  : 検索終了位置
 *  \return  [member:score]な配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZRevRangeAll(c *gin.Context, key string) ([]map[string]int, err.ErrWriter) {
	return this.ZRevRange(c, key, 0, -1)
}

/**************************************************************************************************/
/*!
 *  COMMAND : ZREVRANK
 *
 *  ランクが見つからない場合はエラーが返るが、判断を上層で行うこと(v=0)
 *  ex : ZREVRANK, ranking, member
 *
 *  \param   c      : コンテキスト
 *  \param   key    : キー
 *  \param   member : キー内メンバー
 *  \return  RANK_INDEX、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZRevRank(c *gin.Context, key string, member string) (int, err.ErrWriter) {
	ew := err.NewErrWriter()
	conn := this.getReadConnection(c)

	v, e := redis.Int(conn.Do("ZREVRANK", key, member))
	if e != nil {
		return v, ew.Write(e)
	}

	// NOTE : スコアが見つからない場合はエラーが返るが、判断を上層で行う

	return v, ew
}

/**************************************************************************************************/
/*!
 *  COMMAND : ZSCORE
 *
 *  スコアが見つからない場合はエラーが返るが、判断を上層で行うこと(v=0)
 *  ex : ZSCORE, ranking, member
 *
 *  \param   c      : コンテキスト
 *  \param   key    : キー
 *  \param   member : キー内メンバー
 *  \return  score、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) ZScore(c *gin.Context, key string, member string) (int, err.ErrWriter) {
	ew := err.NewErrWriter()
	conn := this.getReadConnection(c)

	v, e := redis.Int(conn.Do("ZSCORE", key, member))
	if e != nil {
		return v, ew.Write(e)
	}

	return v, ew
}

/**************************************************************************************************/
/*!
 *  オプションの確認
 *
 *  typeだけ確認し、fに処理を委譲する
 *
 *  \param   f       : 委譲先
 *  \param   options : 解析前オプション配列
 *  \return  オプション配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) checkOption(f optionFunc, options []interface{}) ([]interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()
	// 何もしない
	if len(options) < 1 {
		return nil, ew
	}
	// 複数は許さない
	if len(options) > 1 {
		return nil, ew.Write("opiton can set only one!!")
	}

	// typeが違うのはダメ
	switch options[0].(type) {
	case RedisOption:
		args, ew := f(options[0].(RedisOption))
		if ew.HasErr() {
			return nil, ew.Write()
		}
		return args, ew
	}

	// ここに到達すべきではない
	return nil, ew.Write("undefined type!!")
}

/**************************************************************************************************/
/*!
 *  読み込み用コネクションの取得
 *
 *  \param   c : コンテキスト
 *  \return  コネクション
 */
/**************************************************************************************************/
func (this *redisRepo) getReadConnection(c *gin.Context) redis.Conn {
	return this.getConnection(c, ckey.RedisRConn)
}

/**************************************************************************************************/
/*!
 *  書き込み用コネクションの取得
 *
 *  \param   c : コンテキスト
 *  \return  コネクション
 */
/**************************************************************************************************/
func (this *redisRepo) getWriteConnection(c *gin.Context) redis.Conn {
	return this.getConnection(c, ckey.RedisWconn)
}

/**************************************************************************************************/
/*!
 *  redisへのコネクションを取得する
 *
 *  \param   c : コンテキスト
 *  \return  redisへのコネクション
 */
/**************************************************************************************************/
func (this *redisRepo) getConnection(c *gin.Context, key string) redis.Conn {
	var conn redis.Conn
	i, ok := c.Get(key)

	// ない場合取得する
	if !ok {
		ctx := c.MustGet(ckey.GContext).(context.Context)
		pool := ctx.Value(ckey.MemdPool).(*redis.Pool)
		conn = pool.Get()
		c.Set(key, conn)
	} else {
		conn = i.(redis.Conn)
	}
	return conn
}

/**************************************************************************************************/
/*!
 *  トランザクション開始
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Multi(c *gin.Context) err.ErrWriter {
	ew := err.NewErrWriter()
	// すでに開始している場合は何もしない
	if this.isTxStart(c) {
		return ew
	}

	conn := this.getWriteConnection(c)

	e := conn.Send("MULTI")
	if e != nil {
		return ew.Write(e)
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, true)
	return ew
}

/**************************************************************************************************/
/*!
 *  更新系コマンド全実行
 *
 *  \param   c : コンテキスト
 *  \return  実行結果、エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Exec(c *gin.Context) (interface{}, err.ErrWriter) {
	ew := err.NewErrWriter()
	// 開始してない場合は何もしない
	if !this.isTxStart(c) {
		return nil, ew
	}

	conn := this.getWriteConnection(c)

	reply, e := conn.Do("EXEC")
	if e != nil {
		return reply, ew.Write(e)
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, false)

	return reply, ew
}

/**************************************************************************************************/
/*!
 *  コマンドキューの破棄
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Discard(c *gin.Context) err.ErrWriter {
	ew := err.NewErrWriter()
	// 開始してない場合は何もしない
	if !this.isTxStart(c) {
		return ew
	}

	conn := this.getWriteConnection(c)

	_, e := conn.Do("DISCARD")
	if e != nil {
		return ew.Write(e)
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, false)

	return ew
}

/**************************************************************************************************/
/*!
 *  Send処理（レコード書き換えコマンドはこれを使う）
 *
 *  \param   c           : コンテキスト
 *  \param   commandName : redisコマンド
 *  \param   args        : コマンドに応じた値
 *  \return  エラー
 */
/**************************************************************************************************/
func (this *redisRepo) send(c *gin.Context, commandName string, args ...interface{}) err.ErrWriter {
	ew := err.NewErrWriter()
	// トランザクションを開始してない場合、開始する
	if !this.isTxStart(c) {
		this.Multi(c)
	}

	conn := this.getWriteConnection(c)
	e := conn.Send(commandName, args...)
	if e != nil {
		return ew.Write(e)
	}
	return ew
}

/**************************************************************************************************/
/*!
 *  トランザクション実行中か
 *
 *  \param   c : コンテキスト
 *  \return  true / false
 */
/**************************************************************************************************/
func (this *redisRepo) isTxStart(c *gin.Context) bool {
	// コンテキストにあるか確認
	i, ok := c.Get(ckey.IsRedisTxStart)
	if ok && i != nil {
		return i.(bool)
	}
	// なければfalse
	return false
}

/**************************************************************************************************/
/*!
 *  クローズ処理
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Close(c *gin.Context) err.ErrWriter {

	// クローズ処理
	closeFunc := func(key string) err.ErrWriter {
		i, ok := c.Get(key)
		if ok {
			conn := i.(redis.Conn)
			e := conn.Close()
			if e != nil {
				return err.NewErrWriter(e)
			}
		}
		return err.NewErrWriter()
	}

	ew := closeFunc(ckey.RedisRConn)
	if ew.HasErr() {
		return ew.Write()
	}
	ew = closeFunc(ckey.RedisWconn)
	if ew.HasErr() {
		return ew.Write()
	}

	return ew
}
