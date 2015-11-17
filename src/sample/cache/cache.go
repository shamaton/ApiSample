package cache

import (
	"errors"
	"strings"
	"time"

	log "github.com/cihub/seelog"
)

type cacheMap map[string]interface{}
type cacheInfoMap map[string]int64

const (
	local int64 = iota
)

var __cache_data = cacheMap{}
var __cache_info = map[string]cacheInfoMap{}

// tableName_key -> {data:data}
// tableName_key -> {expire:10, expired_at:時間}

func Set(category string, key string, data interface{}) {
	strs := []string{category, key}
	uniqueKey := strings.Join(strs, "_")

	// cache_info_mapを作成
	cache_type := local
	expire := int64(10)
	expiredAt := time.Now().Unix() + expire
	ci := cacheInfoMap{"type": cache_type, "expire": expire, "expired_at": expiredAt}
	__cache_info[uniqueKey] = ci

	// data cache
	__cache_data[uniqueKey] = data
}

func Get(category string, key string) (interface{}, error) {

	uniqueKey := getUniqueKey(category, key)
	ci, isValid := __cache_info[uniqueKey]
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
	data, isValid := __cache_data[uniqueKey]
	if !isValid {
		err := errors.New("unique key [" + uniqueKey + "] is invalid!!")
		return nil, err
	}
	return data, nil
}

func getUniqueKey(category, key string) string {
	strs := []string{category, key}
	return strings.Join(strs, "_")
}
