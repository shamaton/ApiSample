package controller

/**************************************************************************************************/
/*!
 *  api.go
 *
 *  APIっぽいサンプル
 *
 */
/**************************************************************************************************/

import (
	"net/http"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/k0kubun/pp"

	"sample/common/err"
	"sample/common/log"
	"sample/common/redis"
	. "sample/conf"
	"sample/model"

	"time"
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

	// JSON from POST
	type PostJSON struct {
		Id uint64 `json:"Id" binding:"required"`
	}

	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e))
		return
	}

	// FIND TEST
	userRepo := model.NewUserRepo()
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user find_by_id error", err.NewErrWriter().Write())
		return
	}
	log.Debug(pp.Println(user))

	// FIND(USE OPTION)
	var option = model.Option{"mode": MODE_W}
	user = userRepo.FindById(c, 2, option)
	if user == nil {
		errorJson(c, "user find_by_id(use option) error", err.NewErrWriter().Write())
		return
	}
	log.Debug("user find(option)", user)

	// FINDS TEST
	userRepo.FindsTest(c)

	ew := err.NewErrWriter("test error")
	ew = ew.Write("this is error!!")
	ew = ew.Write()

	log.Critical(ew.Err()...)

	// COUNT TEST
	whereCond := model.WhereCondition{{"id", "IN", model.In{1, 2, 3}}}
	condition := model.Condition{"where": whereCond}
	option = model.Option{"shard_key": uint64(1)}

	count, ew := userRepo.Count(c, condition, option)
	if ew.HasErr() {
		errorJson(c, "user count error", ew.Write())
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
	// JSON from POST
	type PostJSON struct {
		Name string `json:"Name" binding:"required"`
		//UUID  string `json:"Name" binding:"required"`
	}
	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e).Write())
		return
	}

	// NOTE : 一度しか生成できない
	userId := uint64(4)

	// ユーザ登録するshardを選択して登録
	shardId, ew := model.NewUserShardWeightRepo().ChoiceShardId(c)
	if ew.HasErr() {
		errorJson(c, "shard id create error ", ew.Write())
		return
	}
	log.Info(shardId)

	userShardRepo := model.NewUserShardRepo()
	userShard := &model.UserShard{Id: int(userId), ShardId: shardId}
	ew = userShardRepo.Create(c, userShard)
	if ew.HasErr() {
		errorJson(c, "user shard create error ", ew.Write())
		return
	}
	// シャード生成のため一旦コミット
	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "shard commit error ", ew.Write())
		return
	}

	// レプリ待ち
	time.Sleep(500 * time.Millisecond)

	// CREATE
	userRepo := model.NewUserRepo()

	newUser := &model.User{Id: userId, Name: json.Name}
	ew = userRepo.Create(c, newUser)
	if ew.HasErr() {
		errorJson(c, "user create error ", ew.Write())
		return
	}
	// COMMIT
	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "commit error!! ", ew.Write())
	}

	c.JSON(http.StatusOK, newUser)
}

/**************************************************************************************************/
/*!
 *  user update test api
 */
/**************************************************************************************************/
func TestUserUpdate(c *gin.Context) {
	// JSON from POST
	type PostJSON struct {
		Id       uint64 `json:"Id" binding:"required"`
		AddScore uint   `json:"AddScore" binding:"required"`
	}

	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e).Write())
		return
	}

	userRepo := model.NewUserRepo()

	// レコードがあるか確認
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user not found!!", err.NewErrWriter().Write())
		return
	}

	// UPDATE TEST
	option := model.Option{"for_update": 1}
	user = userRepo.FindById(c, json.Id, option)
	if user == nil {
		errorJson(c, "user not found!!", err.NewErrWriter().Write())
		return
	}
	log.Debug(user)

	// 今のデータをコピーしてスコア更新
	prevUser := *user
	user.Score += json.AddScore

	ew := userRepo.Update(c, user, &prevUser)
	if ew.HasErr() {
		errorJson(c, "user update error!!", ew.Write())
		return
	}

	// COMMIT
	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "commit error!! ", ew.Write())
	}

	c.JSON(http.StatusOK, user)
}

/**************************************************************************************************/
/*!
 *  user item create test api
 */
/**************************************************************************************************/
func TestUserItemCreate(c *gin.Context) {
	// JSON from POST
	type PostJSON struct {
		UserId uint64 `json:"UserId" binding:"required"`
		ItemId int    `json:"ItemId" binding:"required"`
		Num    int    `json:"Num" binding:"required"`
	}

	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e).Write())
		return
	}

	userItemRepo := model.NewUserItemRepo()

	// 確認不要だが
	userItem := userItemRepo.FindByPk(c, json.UserId, json.ItemId)
	log.Debug("userItem -> ", userItem)

	// SAVE TEST
	saveData := &model.UserItem{UserId: json.UserId, ItemId: json.ItemId, Num: json.Num}
	ew := userItemRepo.Save(c, saveData)
	if ew.HasErr() {
		errorJson(c, "user item save error ", ew.Write())
		return
	}

	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "commit error!! ", ew.Write())
	}

	c.JSON(http.StatusOK, saveData)
}

/**************************************************************************************************/
/*!
 *  user item delete test api
 */
/**************************************************************************************************/
func TestUserItemDelete(c *gin.Context) {
	// JSON from POST
	type PostJSON struct {
		UserId uint64 `json:"UserId" binding:"required"`
		ItemId int    `json:"ItemId" binding:"required"`
	}

	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e))
		return
	}

	userItemRepo := model.NewUserItemRepo()

	// 確認して削除
	userItem := userItemRepo.FindByPk(c, json.UserId, json.ItemId, model.Option{"mode": MODE_W})
	if userItem == nil {
		errorJson(c, "not found user item!! ", err.NewErrWriter().Write())
		return
	}

	// LOCK
	userItem = userItemRepo.FindByPk(c, json.UserId, json.ItemId, model.Option{"mode": MODE_W, "for_update": 1})
	if userItem == nil {
		errorJson(c, "not found user item!! ", err.NewErrWriter().Write())
		return
	}

	// DELETE
	ew := userItemRepo.Delete(c, userItem)
	if ew.HasErr() {
		errorJson(c, "user item save error ", ew.Write())
		return
	}

	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "commit error!! ", ew.Write())
	}

	c.JSON(http.StatusOK, gin.H{"message": "delete OK"})
}

/**************************************************************************************************/
/*!
 *  user log create test api
 */
/**************************************************************************************************/
func TestUserLogCreate(c *gin.Context) {
	// JSON from POST
	type PostJSON struct {
		Id    uint64 `json:"Id" binding:"required"`
		Value uint   `json:"Value" binding:"required"`
	}

	var json PostJSON
	e := c.BindJSON(&json)
	if e != nil {
		errorJson(c, "json error", err.NewErrWriter(e).Write())
		return
	}

	// レコードがあるか確認
	userRepo := model.NewUserRepo()
	user := userRepo.FindById(c, json.Id)
	if user == nil {
		errorJson(c, "user not found!!", err.NewErrWriter().Write())
		return
	}

	logRepo := model.NewUserTestLogRepo()

	// SEQUENCE TEST
	// CREATE
	logData := &model.UserTestLog{UserId: json.Id, TestValue: json.Value}
	ew := logRepo.Create(c, logData)
	if ew.HasErr() {
		errorJson(c, "log create error!! ", ew.Write())
		return
	}

	// CREATE MULTI
	var logDatas []model.UserTestLog

	logData1 := model.UserTestLog{UserId: 3, TestValue: 123}
	logData2 := model.UserTestLog{UserId: 3, TestValue: 4567}
	logDatas = append(logDatas, logData1, logData2)
	if ew = logRepo.CreateMulti(c, &logDatas); ew.HasErr() {
		errorJson(c, "log create multi error!! ", ew.Write())
		return
	}

	// COMMIT
	ew = dbCommit(c)
	if ew.HasErr() {
		errorJson(c, "commit error!! ", ew.Write())
	}

	c.JSON(http.StatusOK, gin.H{"message": "log creates done"})
}

/**************************************************************************************************/
/*!
 *  user misc test api
 */
/**************************************************************************************************/
func TestUserMisc(c *gin.Context) {

	l := func(str string, param interface{}) {
		log.Debug(str, " : ", param)
	}

	redisRepo := redis.NewRedisRepo()

	// 期限切れの場合もある
	var oldb string
	redisRepo.Get(c, "test_key3", &oldb)
	l("old_b", oldb)

	// set
	redisRepo.Set(c, "test_key1", 777)
	redisRepo.Set(c, "test_key2", 1234)
	redisRepo.Set(c, "test_key3", "logic test", redis.RedisOption{"NX": true, "EX": 10})

	user := &model.User{Id: 777, Name: "hoge", Score: 123, CreatedAt: time.Now()}
	redisRepo.Set(c, "test_key4", user)

	// 一旦exec
	redisExec(c)

	// getしてみる
	var t int
	var a uint16
	var b string
	var cc model.User
	redisRepo.Get(c, "test_key1", &t)
	redisRepo.Get(c, "test_key2", &a)
	redisRepo.Get(c, "test_key3", &b)
	redisRepo.Get(c, "test_key4", &cc)
	l("t", t)
	l("a", a)
	l("b", b)
	l("c", cc)

	// exists
	res, _ := redisRepo.Exists(c, "test_key1")
	l("exist1", res)
	res, _ = redisRepo.Exists(c, "test_key1", "hoge")
	l("exist2", res)

	// expire
	redisRepo.Set(c, "expire_test", "test")
	redisRepo.Expire(c, "expire_test", 10)

	// expire_at
	expire_at := time.Now().Add(10 * time.Second)
	redisRepo.Set(c, "expire_at_test", "test")
	redisRepo.ExpireAt(c, "expire_at_test", expire_at)

	// ranking
	scores := map[string]int{"a": 2, "b": 1, "c": 4, "d": 3, "e": 5}
	redisRepo.ZAdd(c, "ranking", "f", 10, redis.RedisOption{"NX": true})
	redisRepo.ZAdds(c, "ranking", scores)

	// commit
	redisExec(c)

	score, _ := redisRepo.ZScore(c, "ranking", "a")
	l("ranking score", score)
	rank, _ := redisRepo.ZRevRank(c, "ranking", "a")
	l("now rank is", rank)
	ranking, _ := redisRepo.ZRevRange(c, "ranking", rank-1, rank+1)
	l("ranking", ranking)
	allranking, _ := redisRepo.ZRevRangeAll(c, "ranking")
	l("ranking_all", allranking)

	// 存在しないキーではランクを0で返す
	rank, _ = redisRepo.ZRevRank(c, "ranking", "abbb")
	l("now rank is", rank)

	// discard
	redisRepo.Set(c, "discard_test", 1)
	redisDiscard(c)

	var discard int
	redisRepo.Get(c, "discard_test", &discard)
	l("discard", discard)

	c.JSON(http.StatusOK, gin.H{})
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
	e := json.Unmarshal(tokenData, &d)
	log.Info(d)

	if e != nil {
		errorJson(c, "token test error!! ", err.NewErrWriter(e).Write())
		return
	}

	// sha256
	recv_sha := c.PostForm("sha")
	log.Info(recv_sha)

	hash := hmac.New(sha256.New, []byte("secret_key"))
	hash.Write([]byte("apple"))
	hashsum := fmt.Sprintf("%x", hash.Sum(nil))
	log.Info(hashsum)

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
func errorJson(c *gin.Context, msg string, ew err.ErrWriter) {
	v := append([]interface{}{msg, ":"}, ew.Err()...)
	log.Error(v)
	c.JSON(http.StatusInternalServerError, gin.H{"error": msg})
}
