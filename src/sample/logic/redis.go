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

	"errors"

	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"
)

/**
 * オプション用マップ
 */
type RedisOption map[string]interface{}
type optionFunc func(RedisOption) ([]interface{}, error)

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
func (this *redisRepo) Set(c *gin.Context, key string, value interface{}, options ...interface{}) error {

	// オプションチェック
	optArgs, err := this.checkOption(this.checkSetOption, options)
	if err != nil {
		return err
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
		j, err := json.Marshal(value)
		if err != nil {
			return err
		}
		args = append(args, key, j)

	default:
		args = append(args, key, refVal)
	}

	// optionを後ろにつける
	args = append(args, optArgs...)

	// SET
	err = this.send(c, "SET", args...)
	if err != nil {
		return err
	}

	return nil
}

/**************************************************************************************************/
/*!
 *  SETコマンドのoptionチェック
 *
 *  \param   options : [EX|PX] [NX|XX]
 *  \return  オプション配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) checkSetOption(option RedisOption) ([]interface{}, error) {
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
			return nil, errors.New("invalid key : " + key)
		}
	}

	// 2重渡しチェック
	if setExPx > 1 || setNxXx > 1 {
		return nil, errors.New("invalid option setting!!")
	}

	return args, nil
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
func (this *redisRepo) Get(c *gin.Context, key string, holder interface{}) error {
	conn := this.getReadConnection(c)

	ref := reflect.ValueOf(holder).Elem()

	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := redis.Int64(conn.Do("GET", key))
		if err != nil {
			return err
		}
		ref.SetInt(v)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := redis.Uint64(conn.Do("GET", key))
		if err != nil {
			return err
		}
		ref.SetUint(v)

	case reflect.String:
		v, err := redis.String(conn.Do("GET", key))
		if err != nil {
			return err
		}
		ref.SetString(v)

	case reflect.Struct:
		v, err := redis.Bytes(conn.Do("GET", key))
		if err != nil {
			return err
		}
		json.Unmarshal(v, holder)
	}

	return nil
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
func (this *redisRepo) Exists(c *gin.Context, key string, keys ...string) (bool, error) {
	conn := this.getReadConnection(c)

	// keys...で渡せないのでinterfaceに入れなおす
	var is []interface{}
	is = append(is, key)
	for _, k := range keys {
		is = append(is, k)
	}

	// exec redis
	v, err := redis.Int(conn.Do("EXISTS", is...))
	if err != nil {
		return false, err
	}

	// 返り値と比較
	var isExists bool
	if v == len(is) {
		isExists = true
	}
	return isExists, nil
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
func (this *redisRepo) Expire(c *gin.Context, key string, second int) error {

	err := this.send(c, "EXPIRE", key, second)

	if err != nil {
		return err
	}

	return nil
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
func (this *redisRepo) ExpireAt(c *gin.Context, key string, t time.Time) error {

	err := this.send(c, "EXPIREAT", key, t.Unix())

	if err != nil {
		return err
	}

	return nil
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
func (this *redisRepo) ZAdd(c *gin.Context, key string, member string, score int, options ...interface{}) error {

	// オプションチェック
	optArgs, err := this.checkOption(this.checkZAddOption, options)
	if err != nil {
		return err
	}

	args := []interface{}{key}
	args = append(args, optArgs...)
	args = append(args, score, member)

	err = this.send(c, "ZADD", args...)
	if err != nil {
		return err
	}

	return nil
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
func (this *redisRepo) ZAdds(c *gin.Context, key string, scoreMap map[string]int, options ...interface{}) error {

	// オプションチェック
	optArgs, err := this.checkOption(this.checkZAddOption, options)
	if err != nil {
		return err
	}

	var args []interface{}
	args = append(args, key)

	// optionを付加
	args = append(args, optArgs...)

	for member, score := range scoreMap {
		args = append(args, score, member)
	}

	err = this.send(c, "ZADD", args...)
	if err != nil {
		return err
	}

	return nil
}

/**************************************************************************************************/
/*!
 *  ZADDのオプションチェック
 *
 *  \param   option : [NX|XX] [CH] [INCR]
 *  \return  オプション配列、失敗時エラー
 */
/**************************************************************************************************/
func (this *redisRepo) checkZAddOption(option RedisOption) ([]interface{}, error) {
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
		return nil, errors.New("invalid option setting!!")
	}

	return args, nil
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
func (this *redisRepo) ZRevRange(c *gin.Context, key string, start int, stop int) ([]map[string]int, error) {
	conn := this.getReadConnection(c)

	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	// mapping
	mapArray := []map[string]int{}
	for i := 0; i < len(values); i += 2 {
		m := map[string]int{}
		str, err := redis.String(values[i], nil)
		if err != nil {
			return nil, err
		}
		value, err := redis.Int(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		m[str] = value
		mapArray = append(mapArray, m)
	}

	return mapArray, nil
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
func (this *redisRepo) ZRevRangeAll(c *gin.Context, key string) ([]map[string]int, error) {
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
func (this *redisRepo) ZRevRank(c *gin.Context, key string, member string) (int, error) {
	conn := this.getReadConnection(c)

	v, err := redis.Int(conn.Do("ZREVRANK", key, member))
	if err != nil {
		return v, err
	}

	// NOTE : スコアが見つからない場合はエラーが返るが、判断を上層で行う

	return v, nil
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
func (this *redisRepo) ZScore(c *gin.Context, key string, member string) (int, error) {
	conn := this.getReadConnection(c)

	v, err := redis.Int(conn.Do("ZSCORE", key, member))
	if err != nil {
		return v, err
	}

	return v, nil
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
func (this *redisRepo) checkOption(f optionFunc, options []interface{}) ([]interface{}, error) {
	// 何もしない
	if len(options) < 1 {
		return nil, nil
	}
	// 複数は許さない
	if len(options) > 1 {
		return nil, errors.New("opiton can set only one!!")
	}

	// typeが違うのはダメ
	switch options[0].(type) {
	case RedisOption:
		args, err := f(options[0].(RedisOption))
		if err != nil {
			return nil, err
		}
		return args, nil
	}

	// ここに到達すべきではない
	return nil, errors.New("undefined type!!")
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

func (this *redisRepo) getReadConnection(c *gin.Context) redis.Conn {
	return this.getConnection(c, ckey.RedisRConn)
}

func (this *redisRepo) getWriteConnection(c *gin.Context) redis.Conn {
	return this.getConnection(c, ckey.RedisWconn)
}

/**************************************************************************************************/
/*!
 *  トランザクション開始
 *
 *  \param   c : コンテキスト
 *  \return  エラー
 */
/**************************************************************************************************/
func (this *redisRepo) Multi(c *gin.Context) error {
	// すでに開始している場合は何もしない
	if this.isTxStart(c) {
		return nil
	}

	conn := this.getWriteConnection(c)

	err := conn.Send("MULTI")
	if err != nil {
		return err
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, true)

	return nil
}

// 更新系コマンド全実行
func (this *redisRepo) Exec(c *gin.Context) (interface{}, error) {
	// 開始してない場合は何もしない
	if !this.isTxStart(c) {
		return nil, nil
	}

	conn := this.getWriteConnection(c)

	reply, err := conn.Do("EXEC")
	if err != nil {
		return reply, err
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, false)

	return reply, nil
}

// コマンドキューの破棄
func (this *redisRepo) Discard(c *gin.Context) error {
	// 開始してない場合は何もしない
	if !this.isTxStart(c) {
		return nil
	}

	conn := this.getWriteConnection(c)

	_, err := conn.Do("DISCARD")
	if err != nil {
		return err
	}

	// save context status
	c.Set(ckey.IsRedisTxStart, false)

	return nil
}

func (this *redisRepo) send(c *gin.Context, commandName string, args ...interface{}) error {
	// トランザクションを開始してない場合、開始する
	if !this.isTxStart(c) {
		this.Multi(c)
	}

	conn := this.getWriteConnection(c)
	err := conn.Send(commandName, args...)
	return err
}

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
func (this *redisRepo) Close(c *gin.Context) error {

	// クローズ処理
	closeFunc := func(key string) error {
		i, ok := c.Get(key)
		if ok {
			conn := i.(redis.Conn)
			err := conn.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := closeFunc(ckey.RedisRConn)
	if err != nil {
		return err
	}
	err = closeFunc(ckey.RedisWconn)
	if err != nil {
		return err
	}

	return nil
}
