package controller

import (
	"net/http"
	"sample/model"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"github.com/k0kubun/pp"
	"golang.org/x/net/context"
	db "sample/DBI"
	"time"
)

// JSON from POST
type PostJSON struct {
	Name  string `json:"Name" binding:"required"`
	Score int    `json:"Score" binding:"required"`
}

type postData struct {
	Name  string `json:"Name"`
	Score int    `json:"Score"`
}

func Test(c *gin.Context) {
	defer db.RollBack(c)

	var json PostJSON
	err := c.BindJSON(&json)
	if checkErr(c, err, "json error") {
		return
	}

	ctx := c.Value("globalContext").(context.Context)

	// MEMD TEST
	redisTest(ctx)

	// FIND TEST
	userRepo := model.NewUserRepo()
	user, err := userRepo.FindByID(c, 2)
	if checkErr(c, err, "user error") {
		return
	}
	log.Debug(pp.Println(user))

	var option = model.Option{"mode": db.MODE_R, "shard_id": 2}
	user, err = userRepo.FindByID(c, 3, option)
	if checkErr(c, err, "user error 2nd") {
		return
	}
	log.Debug(user)

	// FINDS TEST
	userRepo.FindsTest(c)

	// UPDATE TEST
	user, err = userRepo.FindByID(c, 3, db.FOR_UPDATE)
	if checkErr(c, err, "user for update error") {
		return
	}
	log.Debug(user)

	prevUser := *user
	user.Score += 100

	err = userRepo.Update(c, user, &prevUser)
	if checkErr(c, err, "user for update error") {
		return
	}

	// DELETE TEST
	err = userRepo.Delete(c, user)
	if checkErr(c, err, "user for delete error") {
		return
	}

	// CREATE TEST
	err = userRepo.Create(c, &prevUser)
	if checkErr(c, err, "user insert error") {
		return
	}

	// CREATE MULTI TEST
	var users []*model.User
	users = append(users, user)
	users = append(users, &prevUser)
	err = userRepo.CreateMulti(c, &users)
	if checkErr(c, err, "user insert multi error") {
		return
	}

	time.Sleep(0 * time.Second)

	db.Commit(c)

	c.JSON(http.StatusOK, user)
}

func TokenTest(c *gin.Context) {

	var hoge postData
	data := c.PostForm("data")
	dd := []byte(data)
	json.Unmarshal(dd, &hoge)
	log.Info(hoge)

	token := c.PostForm("token")
	log.Info(token)

	// tokenをjsonにもどす
	tokenData, _ := base64.StdEncoding.DecodeString(token)

	var d postData
	err := json.Unmarshal(tokenData, &d)
	log.Info(d)

	checkErr(c, err, "token test error")

	// sha256
	recv_sha := c.PostForm("sha")
	log.Info(recv_sha)

	hash := hmac.New(sha256.New, []byte("secret_key"))
	hash.Write([]byte("apple"))
	hashsum := fmt.Sprintf("%x", hash.Sum(nil))
	log.Infof(hashsum)

	if recv_sha == hashsum {
		log.Info("sha correct!!")
	}

	c.JSON(http.StatusOK, gin.H{"message": "hello"})
}

func redisTest(ctx context.Context) {

	redis_pool := ctx.Value("redis").(*redis.Pool)
	redis_conn := redis_pool.Get()

	var err error
	s, err := redis.String(redis_conn.Do("GET", "message"))
	if err != nil {
		log.Error("get message not found...", err)
	} else {
		log.Info(s)
	}

	_, err = redis_conn.Do("SET", "message", "this is value")
	if err != nil {
		log.Error("set message", err)
	}
	_, err = redis_conn.Do("EXPIRE", "message", 10)

	if err != nil {
		log.Error("error expire ", err)
	}

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
