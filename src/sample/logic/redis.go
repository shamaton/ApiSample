package logic

type redisRepo interface {
	Get()
}

func NewRedisRepo() redisRepo {
	return &redisRepoImpl{}
}

type redisRepoImpl struct {
}

func (r *redisRepoImpl) Get() {
}

func (r *redisRepoImpl) get() {

}
