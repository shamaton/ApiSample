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
	"math/rand"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"

	"sample/common/db"
	"sample/common/err"
	"sample/common/redis"
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
	// context
	ctx = context.Background()
	ew := err.NewErrWriter()
	defer db.Close(ctx)

	setLoggerConfig()

	// game config
	ctx = loadGameConfig(ctx)

	// db
	ctx, ew = db.BuildInstances(ctx)
	if ew.HasErr() {
		log.Critical(ew.Err()...)
		log.Critical("init DB failed!!")
		db.Close(ctx)
		os.Exit(1)
	}

	// redis
	ctx = redis.Initialize(ctx)

	router := gin.Default()
	router.Use(Custom())

	// make route
	makeRoute(router)

	err := router.Run(":9999")

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
		defer db.RollBack(c)
		defer redis.Close(c)

		// ランダムシード
		rand.Seed(time.Now().UnixNano())
		c.Set(ckey.SlaveIndex, db.DecideUseSlave())

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
func loadGameConfig(ctx context.Context) context.Context {
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

	ctx = context.WithValue(ctx, ckey.GameConfig, &gameConf)
	return ctx
}
