package model

import (
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

type cacheSetter func(*gin.Context) (interface{}, error)

type cacheI interface {
	SetCache(interface{}, string, string)
	GetCache(string, string) (interface{}, error)
	GetCacheWithSetter(*gin.Context, string, string, cacheSetter) (interface{}, error)
}

func NewCacheRepo() cacheI {
	return &cacheRepo{}
}

type cacheRepo struct {
}

type cacheMap map[string]interface{}
type cacheInfoMap map[string]int64

const (
	local int64 = iota
)

var cacheData = map[string]*cache{}

// デフォルトの期限切れ時間
const defaultExpireSec = 5

// カスタムな設定値を一括で管理する
// key => expire
var customExpireMap = map[string]int64{
//"hoge": 60,
}

type cache struct {
	expireAt int64       //<! 期限
	data     interface{} //<! データ
}

func (this *cacheRepo) SetCache(data interface{}, key string, member string) {
	uniqueKey := this.getUniqueKey(key, member)

	cache := new(cache)

	// set expire
	expire := int64(defaultExpireSec)
	value, ok := customExpireMap[uniqueKey]
	if ok {
		expire = value
	}
	cache.expireAt = time.Now().Unix() + expire
	cache.data = data

	cacheData[uniqueKey] = cache
}


func (this *cacheRepo) GetCache(key string, member string) (interface{}, error) {

	uniqueKey := this.getUniqueKey(key, member)
	cache, ok := cacheData[uniqueKey]
	if !ok {
		log.Info("not found cache")
		return nil, nil
	}

	// 期限切れはエラーではない
	if time.Now().Unix() > cache.expireAt {
		log.Info("cache is expire")
		return nil, nil
	}

	return cache.data, nil
}

/*
func (this *cacheRepo) GetCache(key string, member string) (interface{}, error) {

	uniqueKey := getUniqueKey(key, member)
	ci, isValid := cacheInfo[uniqueKey]
	if !isValid {
		log.Debug("not fuond cache info -------------------> ")
		return nil, nil
	}

	// 期限切れはエラーではない
	expiredAt, isValid := ci["expired_at"]
	if !(isValid && time.Now().Unix() <= expiredAt) {
		log.Debug("expired -------------------> ")
		return nil, nil
	}

	// データ取得
	data, isValid := _cacheData[uniqueKey]
	if !isValid {
		err := errors.New("unique key [" + uniqueKey + "] is invalid!!")
		return nil, err
	}
	return data, nil
}
*/

func (this *cacheRepo) GetCacheWithSetter(c *gin.Context, key string, member string, setter cacheSetter) (interface{}, error) {
	var cData interface{}
	var err error

	// 取得してみる
	cData, err = this.GetCache(key, member)
	if err != nil {
		return nil, err
	}

	// データなしや期限切れの場合
	if cData == nil {
		cData, err = setter(c)
		if err != nil {
			return nil, err
		}
	}

	return cData, nil
}

func (this *cacheRepo) getUniqueKey(key, member string) string {
	strs := []string{key, member}
	return strings.Join(strs, "_")
}
