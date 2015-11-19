package logic

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

func NewRedisRepo() *redisRepo {
	return &redisRepo{}
}

type redisRepo struct {
}

type RedisOption map[string]interface{}

/*
EX seconds -- Set the specified expire time, in seconds.
PX milliseconds -- Set the specified expire time, in milliseconds.
NX -- Only set the key if it does not already exist.
XX -- Only set the key if it already exist.
*/
func (this *redisRepo) Set(c *gin.Context, key string, value interface{}, options ...interface{}) error {
	conn := this.getConnection(c)

	// オプションチェック
	optArgs, err := this.checkOption(this.checkSetOption, options...)
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
	_, err = conn.Do("SET", args...)
	if err != nil {
		return err
	}

	return nil
}

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

func (this *redisRepo) Get(c *gin.Context, key string, holder interface{}) error {
	conn := this.getConnection(c)

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

// 全て存在する場合はtrue
func (this *redisRepo) Exists(c *gin.Context, key string, keys ...string) (bool, error) {
	conn := this.getConnection(c)

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

func (this *redisRepo) Expire(c *gin.Context, key string, second int) (bool, error) {
	conn := this.getConnection(c)

	v, err := redis.Bool(conn.Do("EXPIRE", key, second))
	if err != nil {
		return false, err
	}

	return v, nil
}

func (this *redisRepo) ExpireAt(c *gin.Context, key string, t time.Time) (bool, error) {
	conn := this.getConnection(c)

	v, err := redis.Bool(conn.Do("EXPIREAT", key, t.Unix()))
	if err != nil {
		return false, err
	}

	return v, nil
}

/////////////////////////////

// ZADD [NX]XX] key score member
func (this *redisRepo) ZAdd(c *gin.Context, key string, member string, score int, options ...interface{}) (int, error) {
	conn := this.getConnection(c)

	// オプションチェック
	optArgs, err := this.checkOption(this.checkZAddOption, options...)
	if err != nil {
		return 0, err
	}

	args := []interface{}{key}
	args = append(args, optArgs...)
	args = append(args, score, member)

	v, err := redis.Int(conn.Do("ZADD", args...))
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (this *redisRepo) ZAdds(c *gin.Context, key string, scoreMap map[string]int, options ...interface{}) (int, error) {
	conn := this.getConnection(c)

	// オプションチェック
	optArgs, err := this.checkOption(this.checkZAddOption, options...)
	if err != nil {
		return 0, err
	}

	var args []interface{}
	args = append(args, key)

	// optionを付加
	args = append(args, optArgs...)

	for member, score := range scoreMap {
		args = append(args, score, member)
	}

	v, err := redis.Int(conn.Do("ZADD", args...))
	if err != nil {
		return 0, err
	}

	return v, nil
}

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

func (this *redisRepo) ZRevRange(c *gin.Context, key string, start int, stop int) ([]map[string]int, error) {
	conn := this.getConnection(c)

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

func (this *redisRepo) ZRevRangeAll(c *gin.Context, key string) ([]map[string]int, error) {
	return this.ZRevRange(c, key, 0, -1)
}

func (this *redisRepo) ZRevRank(c *gin.Context, key string, member string) (int, error) {
	conn := this.getConnection(c)

	v, err := redis.Int(conn.Do("ZREVRANK", key, member))
	if err != nil {
		return v, err
	}

	// NOTE : スコアが見つからない場合はエラーが返るが、判断を上層で行う

	return v, nil
}

func (this *redisRepo) ZScore(c *gin.Context, key string, member string) (int, error) {
	conn := this.getConnection(c)

	v, err := redis.Int(conn.Do("ZSCORE", key, member))
	if err != nil {
		return v, err
	}

	// NOTE : スコアが見つからない場合はエラーが返るが、判断を上層で行う

	return v, nil
}

func (this *redisRepo) checkOption(f func(RedisOption) ([]interface{}, error), options ...interface{}) ([]interface{}, error) {
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

func (this *redisRepo) getConnection(c *gin.Context) redis.Conn {
	ctx := c.MustGet(ckey.GContext).(context.Context)
	pool := ctx.Value(ckey.MemdPool).(*redis.Pool)
	conn := pool.Get()
	return conn
}
