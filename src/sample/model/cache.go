package model

import (
	"errors"
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

var cacheData = cacheMap{}
var cacheInfo = map[string]cacheInfoMap{}

// tableName_key -> {data:data}
// tableName_key -> {expire:10, expired_at:時間}

func (this *cacheRepo) SetCache(data interface{}, key string, member string) {
	strs := []string{key, member}
	uniqueKey := strings.Join(strs, "_")

	// cache_info_mapを作成
	cache_type := local
	expire := int64(10)
	expiredAt := time.Now().Unix() + expire
	ci := cacheInfoMap{"type": cache_type, "expire": expire, "expired_at": expiredAt}
	cacheInfo[uniqueKey] = ci

	// data cache
	cacheData[uniqueKey] = data
}

func (this *cacheRepo) GetCache(key string, member string) (interface{}, error) {

	uniqueKey := getUniqueKey(key, member)
	ci, isValid := cacheInfo[uniqueKey]
	if !isValid {
		log.Debug("not fuond cache info -------------------> ")
		//err = errors.New("unique key ["+uniqueKey+"] is invalid!!")
		return nil, nil
	}

	// 期限切れはエラーではない
	expiredAt, isValid := ci["expired_at"]
	if !(isValid && time.Now().Unix() <= expiredAt) {
		log.Debug("expired -------------------> ")
		return nil, nil
	}

	// typeごとに取得

	// データ取得
	data, isValid := cacheData[uniqueKey]
	if !isValid {
		err := errors.New("unique key [" + uniqueKey + "] is invalid!!")
		return nil, err
	}
	return data, nil
}

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

func getUniqueKey(key, member string) string {
	strs := []string{key, member}
	return strings.Join(strs, "_")
}
