package controller

import (
	"hoge"
	"model"
	"net/http"

	log "github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"
	"golang.org/x/net/context"
)

// JSON from POST
type PostJSON struct {
	Name  string `json:"Name" binding:"required"`
	Score int    `json:"Score" binding:"required"`
}

func Test(c *gin.Context) {

	var json PostJSON
	err := c.BindJSON(&json)
	if checkErr(c, err, "json error") {
		return
	}

	ctx := c.Value("gContext").(context.Context)

	// データをselect
	res := model.Find(c, 3)
	log.Info(res)

	// use redis
	redisTest(ctx)

	// データをupdate
	var h *xorm.Engine
	h, err = hoge.GetDBConnection(c, "user")

	session := h.NewSession()
	defer session.Close()

	err = session.Begin()
	if checkErr(c, err, "begin error") {
		return
	}

	var u []model.User
	err = session.Where("id = ?", 3).ForUpdate().Find(&u)
	if checkErr(c, err, "user not found") {
		return
	}

	user := u[0]
	user.Score += 1

	//time.Sleep(3 * time.Second)

	//res, e := session.Id(user.Id).Cols("score").Update(&user) // 単一 PK
	_, err = session.Id(core.PK{user.Id, user.Name}).Update(&user) // 複合PK
	if checkErr(c, err, "update error") {
		return
	}

	err = session.Commit()
	if checkErr(c, err, "commit error") {
		return
	}

	c.JSON(http.StatusOK, &user)
}

func redisTest(ctx context.Context) {

	redis_pool := ctx.Value("redis").(*redis.Pool)
	redis_conn := redis_pool.Get()

	_, e2 := redis_conn.Do("SET", "message", "this is value")
	if e2 != nil {
		log.Error("set message", e2)
	}
	s, err := redis.String(redis_conn.Do("GET", "message"))
	if err != nil {
		log.Error("get message", err)
	}
	log.Info("%#v\n", s)
}

// エラー表示
func checkErr(c *gin.Context, err error, msg string) bool {
	if err != nil {
		log.Error(msg, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": msg})
		return true
	}
	return false
}
