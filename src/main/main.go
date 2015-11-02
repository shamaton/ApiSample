package main

import (
	"controller"
	"hoge"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"

	log "github.com/cihub/seelog"

	"github.com/BurntSushi/toml"
	"github.com/garyburd/redigo/redis"
	"os"
)

type gameConfig struct {
	Server ServerConfig
	Db     DbConfig
	Kvs    KvsConfig
}

type ServerConfig struct {
	Host  string        `toml:"host"`
	Port  string        `toml:"port"`
	Slave []SlaveServer `toml:"slave"`
}

type SlaveServer struct {
	Weight int    `toml:"weight"`
	Ip     string `toml:"ip"`
}

type DbConfig struct {
	User string `toml:"user"`
	Pass string `toml:"pass"`
}

type KvsConfig struct {
	Host string `toml:"host"`
	Port string `toml:"port"`
}

// global
var (
	ctx context.Context
)

// redis ConnectionPooling
func newPool(gameConf *gameConfig) *redis.Pool {
	// KVSのpoolを取得
	return &redis.Pool{

		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", gameConf.Kvs.Host+":"+gameConf.Kvs.Port)

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

func setLoggerConfig() {
	// PJ直下で実装した場合
	logger, err := log.LoggerFromConfigAsFile("./conf/seelog/development.xml")

	if err != nil {
		panic("fail to load logger setting")
	}

	log.ReplaceLogger(logger)

}

func loadGameConfig() *gameConfig {
	var gameConf gameConfig

	gameMode := os.Getenv("GAMEMODE")

	// config load
	switch gameMode {
	case "PRODUCTION":
		log.Info("SET PRODUCTION MODE...")

	case "DEVELOPMENT":
		log.Info("SET DEVELOPMENT MODE...")

	default:
		log.Info("SET LOCAL MODE...")

		_, err := toml.DecodeFile("./conf/game/local.toml", &gameConf)
		if err != nil {
			log.Critical("gameConf local.toml error!!", err)
			os.Exit(1)
		}
	}

	return &gameConf
}

func main() {
	// context
	ctx = context.Background()

	setLoggerConfig()

	// game config
	gameConf := loadGameConfig()
	ctx = context.WithValue(ctx, "gameConf", gameConf)

	// db
	hoge.BuildInstances()

	// redis
	redis_pool := newPool(gameConf)
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
