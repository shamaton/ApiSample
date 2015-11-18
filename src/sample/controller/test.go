package controller

import (
	"net/http"
	"sample/model"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	db "sample/DBI"

	ckey "sample/conf/context"

	"time"

	"sample/logic"

	log "github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"github.com/k0kubun/pp"
	"golang.org/x/net/context"
)

type postData struct {
	Name  string `json:"Name"`
	Score int    `json:"Score"`
}

/**************************************************************************************************/
/*!
 *  user select test api
 */
/**************************************************************************************************/
func TestUserSelect(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		Id uint64 `json:"Id" binding:"required"`
	}

	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	// FIND TEST
	userRepo := model.NewUserRepo()
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user find_by_id error", nil)
		return
	}
	log.Debug(pp.Println(user))

	// FIND(USE OPTION)
	var option = model.Option{"mode": db.MODE_W}
	user = userRepo.FindById(c, 3, option)
	if user == nil {
		errorJson(c, "user find_by_id(use option) error", nil)
		return
	}
	log.Debug("user find(option)", user)

	// FINDS TEST
	userRepo.FindsTest(c)

	// COUNT TEST
	whereCond := model.WhereCondition{{"id", "IN", model.In{1, 2, 3}}}
	condition := model.Condition{"where": whereCond}
	option = model.Option{"shard_key": uint64(1)}

	count, err := userRepo.Count(c, condition, option)
	if err != nil {
		errorJson(c, "user count error", err)
		return
	}
	log.Debug("user count : ", count)

	c.JSON(http.StatusOK, user)
}

/**************************************************************************************************/
/*!
 *  user create test api
 */
/**************************************************************************************************/
func TestUserCreate(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		Name string `json:"Name" binding:"required"`
		//UUID  string `json:"Name" binding:"required"`
	}
	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	// NOTE : 一度しか生成できない
	userId := uint64(4)

	// ユーザ登録するshardを選択して登録
	shardId := 1

	userShardRepo := model.NewUserShardRepo()
	userShard := &model.UserShard{Id: int(userId), ShardId: shardId}
	err = userShardRepo.Create(c, userShard)
	if err != nil {
		errorJson(c, "user shard create error ", err)
		return
	}
	// シャード生成のため一旦コミット
	db.Commit(c)

	// レプリ待ち
	time.Sleep(500 * time.Millisecond)

	// CREATE
	userRepo := model.NewUserRepo()

	newUser := &model.User{Id: userId, Name: json.Name}
	err = userRepo.Create(c, newUser)
	if err != nil {
		errorJson(c, "user create error ", err)
		return
	}
	// COMMIT
	db.Commit(c)

	c.JSON(http.StatusOK, newUser)
}

/**************************************************************************************************/
/*!
 *  user update test api
 */
/**************************************************************************************************/
func TestUserUpdate(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		Id       uint64 `json:"Id" binding:"required"`
		AddScore uint   `json:"AddScore" binding:"required"`
	}

	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	userRepo := model.NewUserRepo()

	// レコードがあるか確認
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user not found!!", nil)
		return
	}

	// UPDATE TEST
	option := model.Option{"for_update": 1}
	user = userRepo.FindById(c, json.Id, option)
	if user == nil {
		errorJson(c, "user not found!!", nil)
		return
	}
	log.Debug(user)

	// 今のデータをコピーしてスコア更新
	prevUser := *user
	user.Score += json.AddScore

	err = userRepo.Update(c, user, &prevUser)
	if err != nil {
		errorJson(c, "user update error!!", err)
		return
	}
	// COMMIT
	db.Commit(c)

	c.JSON(http.StatusOK, user)
}

/**************************************************************************************************/
/*!
 *  user item create test api
 */
/**************************************************************************************************/
func TestUserItemCreate(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		UserId uint64 `json:"UserId" binding:"required"`
		ItemId int    `json:"ItemId" binding:"required"`
		Num    int    `json:"Num" binding:"required"`
	}

	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	userItemRepo := model.NewUserItemRepo()

	// 確認不要だが
	userItem := userItemRepo.FindByPk(c, json.UserId, json.ItemId)
	log.Debug("userItem -> ", userItem)

	// SAVE TEST
	saveData := &model.UserItem{UserId: json.UserId, ItemId: json.ItemId, Num: json.Num}
	err = userItemRepo.Save(c, saveData)
	if err != nil {
		errorJson(c, "user item save error ", err)
		return
	}

	db.Commit(c)

	c.JSON(http.StatusOK, saveData)
}

/**************************************************************************************************/
/*!
 *  user item delete test api
 */
/**************************************************************************************************/
func TestUserItemDelete(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		UserId uint64 `json:"UserId" binding:"required"`
		ItemId int    `json:"ItemId" binding:"required"`
	}

	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	userItemRepo := model.NewUserItemRepo()

	// 確認して削除
	userItem := userItemRepo.FindByPk(c, json.UserId, json.ItemId, model.Option{"mode": db.MODE_W})
	if userItem == nil {
		errorJson(c, "not found user item!! ", nil)
		return
	}

	// LOCK
	userItem = userItemRepo.FindByPk(c, json.UserId, json.ItemId, model.Option{"mode": db.MODE_W, "for_update": 1})
	if userItem == nil {
		errorJson(c, "not found user item!! ", nil)
		return
	}

	// DELETE
	err = userItemRepo.Delete(c, userItem)
	if err != nil {
		errorJson(c, "user item save error ", err)
		return
	}

	db.Commit(c)

	c.JSON(http.StatusOK, gin.H{"message": "delete OK"})
}

/**************************************************************************************************/
/*!
 *  user log create test api
 */
/**************************************************************************************************/
func TestUserLogCreate(c *gin.Context) {
	defer db.RollBack(c)

	// JSON from POST
	type PostJSON struct {
		Id    uint64 `json:"Id" binding:"required"`
		Value uint   `json:"Value" binding:"required"`
	}

	var json PostJSON
	err := c.BindJSON(&json)
	if err != nil {
		errorJson(c, "json error", err)
		return
	}

	// レコードがあるか確認
	userRepo := model.NewUserRepo()
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user not found!!", nil)
		return
	}

	logRepo := model.NewUserTestLogRepo()

	// SEQUENCE TEST
	// CREATE
	logData := &model.UserTestLog{UserId: json.Id, TestValue: json.Value}
	err = logRepo.Create(c, logData)
	if err != nil {
		errorJson(c, "log create error!! ", err)
		return
	}

	// CREATE MULTI
	var logDatas []model.UserTestLog

	logData1 := model.UserTestLog{UserId: 3, TestValue: 123}
	logData2 := model.UserTestLog{UserId: 3, TestValue: 4567}
	logDatas = append(logDatas, logData1, logData2)
	if err = logRepo.CreateMulti(c, &logDatas); err != nil {
		errorJson(c, "log create multi error!! ", err)
		return
	}

	// COMMIT
	db.Commit(c)

	c.JSON(http.StatusOK, gin.H{"message": "log creates done"})
}

/**************************************************************************************************/
/*!
 *  user misc test api
 */
/**************************************************************************************************/
func TestUserMisc(c *gin.Context) {
	defer db.RollBack(c)

	ctx := c.Value(ckey.GContext).(context.Context)

	// MEMD TEST
	redisTest(ctx)

	redisRepo := logic.NewRedisRepo()
	redisRepo.Set(c, "test_key", 777)
	redisRepo.Set(c, "test_key2", 1234)
	redisRepo.Set(c, "test_key3", "logic test")

	user := &model.User{Id: 777, Name: "hoge", Score: 123, CreatedAt: time.Now()}
	redisRepo.Set(c, "test_key4", user)

	var hoge int
	redisRepo.Get(c, "test_key", &hoge)
	log.Debug("hoge ---------------> ", hoge)

	var a uint16
	redisRepo.Get(c, "test_key2", &a)
	log.Debug("a ---------------> ", a)

	var b string
	redisRepo.Get(c, "test_key3", &b)
	log.Debug("b ---------------> ", b)

	var cc model.User
	redisRepo.Get(c, "test_key4", &cc)
	log.Debug("cc ---------------> ", cc)

	c.JSON(http.StatusOK, gin.H{})
}

func redisTest(ctx context.Context) {

	redis_pool := ctx.Value(ckey.MemdPool).(*redis.Pool)
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

	// 全体
	allrank, _ := redis.Strings(redis_conn.Do("ZREVRANGE", "ranking_test", 0, -1))
	log.Debug(allrank)

	// スコア
	score, _ := redis.Int(redis_conn.Do("ZSCORE", "ranking_test", "d"))
	log.Debug(score)

	// ランク
	myrank, _ := redis.Int(redis_conn.Do("ZREVRANK", "ranking_test", "d"))
	log.Debug(myrank)

	// struct -> JSON
	user := &model.User{Id: 777, Name: "hoge", Score: 123, CreatedAt: time.Now()}
	serialized, _ := json.Marshal(user)
	log.Debug("seli -------------------> ", string(serialized))

	// JSON -> struct
	deserialized := new(model.User)
	json.Unmarshal(serialized, deserialized)
	log.Debug("dese -------------------> ", deserialized)

	//
	jsontest, _ := redis.Bytes(redis_conn.Do("GET", "jsontest"))
	log.Debug("jsontest ---------> ", jsontest)
	if jsontest != nil {
		dejson := new(model.User)
		json.Unmarshal(serialized, dejson)
		log.Debug("jsontest ---------> ", dejson)
	}

	redis_conn.Do("SET", "jsontest", serialized, "EX", 10)
}

/**************************************************************************************************/
/*!
 *  token test api
 */
/**************************************************************************************************/
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

	if err != nil {
		errorJson(c, "token test error!! ", err)
		return
	}

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

/**************************************************************************************************/
/*!
 *  エラー投げる
 */
/**************************************************************************************************/
func errorJson(c *gin.Context, msg string, err error) {
	log.Error(msg, " : ", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": msg})
}
