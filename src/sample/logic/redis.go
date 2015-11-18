package logic

import (
	"reflect"
	ckey "sample/conf/context"

	"encoding/json"

	log "github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"
)

func NewRedisRepo() *redisRepo {
	return &redisRepo{}
}

type redisRepo struct {
}

func (this *redisRepo) Set(c *gin.Context, key string, value interface{}, options ...interface{}) error {
	conn := this.getConnection(c)

	var refVal reflect.Value
	ref := reflect.ValueOf(value)

	log.Error(ref.Kind())

	if ref.Kind() == reflect.Ptr {
		refVal = ref.Elem()
	} else {
		refVal = ref
	}

	switch refVal.Kind() {
	case reflect.Struct:
		j, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = conn.Do("SET", key, j, "EX", 10)
		if err != nil {
			return err
		}

	default:
		_, err := conn.Do("SET", key, refVal, "EX", 10)
		if err != nil {
			return err
		}
	}

	return nil
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

func (this *redisRepo) getConnection(c *gin.Context) redis.Conn {
	ctx := c.MustGet(ckey.GContext).(context.Context)
	pool := ctx.Value(ckey.MemdPool).(*redis.Pool)
	conn := pool.Get()
	return conn
}
