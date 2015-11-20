package model

import (
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

// デフォルトの期限切れ時間
const defaultExpireSec = 5 * 60

/**
 * カスタムな設定値を一括で管理する
 * key : expire
 */
var customExpireMap = map[string]int64{
//"hoge": 60,
}

/**
 * キャッシュ保存用構造体
 */
type cache struct {
	expireAt int64       //<! 期限
	data     interface{} //<! データ
}

var cacheData = map[string]*cache{}

/**
 * cache setter : 処理委譲用
 */
type cacheSetter func(*gin.Context) (interface{}, error)

/**
 * interface
 */
type cacheI interface {
	SetCache(interface{}, string, ...string)
	GetCache(string, ...string) (interface{}, error)
	GetCacheWithSetter(*gin.Context, cacheSetter, string, ...string) (interface{}, error)
}

/**************************************************************************************************/
/*!
 *  操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewCacheRepo() cacheI {
	return &cacheRepo{}
}

/**
 * cache accessor
 */
type cacheRepo struct {
}

/**************************************************************************************************/
/*!
 *  キャッシュをセットする
 *
 *  \param   key     : 主キー
 *  \param   members : 副キー
 *  \return  なし
 */
/**************************************************************************************************/
func (this *cacheRepo) SetCache(data interface{}, key string, members ...string) {
	uniqueKey := this.getUniqueKey(key, members)

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

/**************************************************************************************************/
/*!
 *  キャッシュを取得する
 *
 *  \param   key     : 主キー
 *  \param   members : 副キー
 *  \return  キャッシュ、エラー
 */
/**************************************************************************************************/
func (this *cacheRepo) GetCache(key string, members ...string) (interface{}, error) {

	uniqueKey := this.getUniqueKey(key, members)
	cache, ok := cacheData[uniqueKey]
	if !ok {
		log.Info("not found cache")
		return nil, nil
	}

	// 期限切れ時は古いのを削除しておく
	if time.Now().Unix() > cache.expireAt {
		log.Info("cache is expire")
		delete(cacheData, uniqueKey)
		return nil, nil
	}

	return cache.data, nil
}

/**************************************************************************************************/
/*!
 *  キャッシュを取得する。存在しない場合はcacheSetterを呼ぶ
 *
 *  \param   c       : コンテキスト
 *  \param   setter  : cacheSetter
 *  \param   key     : 主キー
 *  \param   members : 副キー
 *  \return  キャッシュ、エラー
 */
/**************************************************************************************************/
func (this *cacheRepo) GetCacheWithSetter(c *gin.Context, setter cacheSetter, key string, members ...string) (interface{}, error) {
	var cData interface{}
	var err error

	// 取得してみる
	cData, err = this.GetCache(key, members...)
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

/**************************************************************************************************/
/*!
 *  keyとmembersからユニークなキーを取得する
 *
 *  \param   key     : 主キー
 *  \param   members : 副キー
 *  \return  ユニークキー
 */
/**************************************************************************************************/
func (this *cacheRepo) getUniqueKey(key string, members []string) string {
	strs := []string{key}
	strs = append(strs, members...)
	return strings.Join(strs, "_")
}
