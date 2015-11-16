package model

import (
	"time"

	"github.com/gin-gonic/gin"
)

type UserTestLog struct {
	Id        uint64    `pk:"true" seq:"true"`
	UserId    uint64    `db:"user_id" shard:"true"`
	TestValue uint      `db:"test_value"`
	CreatedAt time.Time `db:"created_at"`
}

type UserTestLogRepo interface {
	FindByID(*gin.Context, uint64, ...interface{}) *UserTestLog

	Create(*gin.Context, *UserTestLog) error
	CreateMulti(*gin.Context, *[]*UserTestLog) error

	Delete(*gin.Context, interface{}) error
}

func NewUserTestLogRepo() UserTestLogRepo {
	b := &base{table: "user_test_log"}
	return UserTestLogRepoImpl{b}
}

type UserTestLogRepoImpl struct {
	*base
}

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

func (r UserTestLogRepoImpl) Create(c *gin.Context, userTestLog *UserTestLog) error {
	err := r.base.Create(c, userTestLog)
	return err
}

func (r UserTestLogRepoImpl) CreateMulti(c *gin.Context, userTestLogs *[]*UserTestLog) error {
	err := r.base.CreateMulti(c, userTestLogs)
	return err
}
