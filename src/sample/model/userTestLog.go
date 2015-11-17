package model

import (
	"time"

	"github.com/gin-gonic/gin"
)


/**
 * \struct UserTestLog
 * \brief テーブル定義
 */
type UserTestLog struct {
	Id        uint64    `pk:"t" seq:"t"`
	UserId    uint64    `db:"user_id" shard:"t"`
	TestValue uint      `db:"test_value"`
	CreatedAt time.Time `db:"created_at"`
}

/**
 * interface
 */
type UserTestLogRepo interface {
	FindByID(*gin.Context, uint64, ...interface{}) *UserTestLog

	Create(*gin.Context, *UserTestLog) error
	CreateMulti(*gin.Context, *[]UserTestLog) error

	Delete(*gin.Context, interface{}) error
}

/**************************************************************************************************/
/*!
 *  リポジトリ操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserTestLogRepo() UserTestLogRepo {
	b := &base{table: "user_test_log"}
	return UserTestLogRepoImpl{b}
}

/**
 * implementer
 */
type UserTestLogRepoImpl struct {
	*base
}

/**************************************************************************************************/
/*!
 *  PRIMARY KEYで検索する
 *
 *  \param   c       : コンテキスト
 *  \param   id      : ログID
 *  \param   options : オプション
 *  \return  ログ(エラー時はnil)
 */
/**************************************************************************************************/
func (r UserTestLogRepoImpl) FindByID(c *gin.Context, id uint64, options ...interface{}) *UserTestLog {
	var userTestLog = new(UserTestLog)
	userTestLog.Id = id
	userTestLog.UserId = 1 // test
	err := r.Find(c, userTestLog, options...)
	if err != nil {
		return nil
	}
	return userTestLog
}

/**************************************************************************************************/
/*!
 *  ログを挿入する
 *
 *  \param   c           : コンテキスト
 *  \param   userTestLog : ログデータ
 *  \return  エラー
 */
/**************************************************************************************************/
func (r UserTestLogRepoImpl) Create(c *gin.Context, userTestLog *UserTestLog) error {
	err := r.base.Create(c, userTestLog)
	return err
}

/**************************************************************************************************/
/*!
 *  ログを複数挿入する
 *
 *  \param   c            : コンテキスト
 *  \param   userTestLogs : ログデータ
 *  \return  エラー
 */
/**************************************************************************************************/
func (r UserTestLogRepoImpl) CreateMulti(c *gin.Context, userTestLogs *[]UserTestLog) error {
	err := r.base.CreateMulti(c, userTestLogs)
	return err
}
