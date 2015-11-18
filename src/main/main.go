package main

/**************************************************************************************************/
/*!
 *  main.go
 *
 *  アプリエントリポイント
 *
 */
/**************************************************************************************************/
import (
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"

	log "github.com/cihub/seelog"

	"math/rand"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/garyburd/redigo/redis"

	"sample/DBI"
	ckey "sample/conf/context"
	"sample/conf/gameConf"
)

// global
var (
	ctx context.Context
)

/**************************************************************************************************/
/*!
 *  main
 */
/**************************************************************************************************/
func main() {
	var err error
	// context
	ctx = context.Background()

	setLoggerConfig()

	// game config
	gameConf := loadGameConfig()
	ctx = context.WithValue(ctx, ckey.GameConfig, gameConf)

	// db
	ctx, err = DBI.BuildInstances(ctx)
	if err != nil {
		log.Critical("init DB failed!!")
		os.Exit(1)
	}

	// redis
	redis_pool := newPool(gameConf)
	ctx = context.WithValue(ctx, ckey.MemdPool, redis_pool)

	router := gin.Default()
	router.Use(Custom())

	// make route
	makeRoute(router)

	err = router.Run(":9999")

	// 存在しないルート時
	if err != nil {
		log.Critical(err)
	}
}

/**************************************************************************************************/
/*!
 *  routing
 */
/**************************************************************************************************/
func makeRoute(router *gin.Engine) {
	// POST
	for k, v := range routerPostConf {
		router.POST("/"+k, v)
	}

	// GET
	for k, v := range routerGetConf {
		router.POST("/"+k, v)
	}
}

/**************************************************************************************************/
/*!
 *  リクエスト毎の処理のカスタム
 *
 *  \return  ハンドラ
 */
/**************************************************************************************************/
func Custom() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := time.Now()

		// set global context
		c.Set(ckey.GContext, ctx)

		// リクエスト前処理
		defer log.Flush()

		// ランダムシード
		rand.Seed(time.Now().UnixNano())
		c.Set(ckey.SlaveIndex, DBI.DecideUseSlave())

		c.Next()

		// リクエスト後処理
		latency := time.Since(t)
		log.Info("latency : ", latency)

		// access the status we are sending
		// status := c.Writer.Status()
		// log.Info(status)
	}
}

/**************************************************************************************************/
/*!
 *  loggerの設定
 */
/**************************************************************************************************/
func setLoggerConfig() {
	// PJ直下で実装した場合
	logger, err := log.LoggerFromConfigAsFile("./conf/seelog/development.xml")

	if err != nil {
		panic("fail to load logger setting")
	}

	log.ReplaceLogger(logger)

}

/**************************************************************************************************/
/*!
 *  アプリの設定をロードする
 *
 *  \return  gameConfig
 */
/**************************************************************************************************/
func loadGameConfig() *gameConf.GameConfig {
	var gameConf gameConf.GameConfig

	gameMode := os.Getenv("GAMEMODE")

	// config load
	var filename string
	switch gameMode {
	case "PRODUCTION":
		log.Info("SET PRODUCTION MODE...")

	case "DEVELOPMENT":
		log.Info("SET DEVELOPMENT MODE...")

	default:
		log.Info("SET LOCAL MODE...")
		filename = "local"
	}

	_, err := toml.DecodeFile("./conf/game/"+filename+".toml", &gameConf)
	if err != nil {
		log.Critical("gameConf "+filename+".toml error!!", err)
		os.Exit(1)
	}

	return &gameConf
}

/**************************************************************************************************/
/*!
 *  redisのプールを取得
 *
 *  \param   gameConf : ゲームの設定
 *  \return  プール
 */
/**************************************************************************************************/
func newPool(gameConf *gameConf.GameConfig) *redis.Pool {
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
