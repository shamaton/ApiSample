package model

/**************************************************************************************************/
/*!
 *  userTestLog.go
 *
 *  table : user_test_logのmodelクラス
 *
 */
/**************************************************************************************************/
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
 * db accessor
 */
type userTestLogRepo struct {
	baseI
}

/**************************************************************************************************/
/*!
 *  DB操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewUserTestLogRepo() *userTestLogRepo {
	b := NewBase("user_test_log")
	return &userTestLogRepo{b}
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
func (this *userTestLogRepo) FindByID(c *gin.Context, id uint64, options ...interface{}) *UserTestLog {
	var userTestLog = new(UserTestLog)
	userTestLog.Id = id
	userTestLog.UserId = 1 // test
	err := this.Find(c, userTestLog, options...)
	if err != nil {
		return nil
	}
	return userTestLog
}
