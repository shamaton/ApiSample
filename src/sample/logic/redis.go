package logic

import "sample/model"

type redisRepo interface {
	Get()
}

func NewRedisRepo() redisRepo {
	b := model.NewBase("hgoe")
	return &redisRepoImpl{base: b}
}

type redisRepoImpl struct {
	base model.Base
}

func (r *redisRepoImpl) Get() {
}

func (r *redisRepoImpl) get() {

}
