package main

import (
	"controller"
	"hoge"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"

	log "github.com/cihub/seelog"

	"github.com/garyburd/redigo/redis"
)

// global
var (
	ctx context.Context
)

// redis ConnectionPooling
func newPool() *redis.Pool {
	hostname := "127.0.0.1"
	port := "6379"

	return &redis.Pool{

		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", hostname+":"+port)

			if err != nil {
				return nil, err
			}
			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func Custom() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := time.Now()

		// set global context
		c.Set("gContext", ctx)

		// リクエスト前処理
		defer log.Flush()

		c.Next()

		// リクエスト後処理
		latency := time.Since(t)
		log.Info(latency)

		// access the status we are sending
		status := c.Writer.Status()
		log.Info(status)
	}
}

func loadConfig() {
	// PJ直下で実装した場合
	logger, err := log.LoggerFromConfigAsFile("./conf/seelog/development.xml")

	if err != nil {
		panic("fail to load config")
	}

	log.ReplaceLogger(logger)
}

func main() {
	// context
	ctx = context.Background()

	loadConfig()

	// db
	hoge.BuildInstances()

	// redis
	redis_pool := newPool()
	ctx = context.WithValue(ctx, "redis", redis_pool)

	router := gin.Default()
	router.Use(Custom())

	// make route
	router.POST("/test", controller.Test)

	err := router.Run(":9999")

	// 存在しないルート時
	if err != nil {
		log.Critical(err)
	}
}

// エラー表示
func checkErr(err error, msg string) {
	if err != nil {
		log.Error(msg, err)
	}
}
