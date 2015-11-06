package model2

/*
import (
	"github.com/gin-gonic/gin"
	"github.com/go-xorm/xorm"
	"sample/DBI"
	"golang.org/x/net/context"
)

type DBMAP map[int]*xorm.Engine
type TXMAP map[int]*xorm.Session

type BaseRepo interface {
	FindByID(int) (*User, error)
}

func NewUserRepo(c *gin.Context) UserRepo {

	gc := c.Value("globalContext").(context.Context)
	shardWMap := gc.Value("dbShardWMap").(map[int]*xorm.Engine)

	slaveIndex := c.Value("slaveIndex").(int)
	dbShardRMaps := gc.Value("dbShardRMaps").([]map[int]*xorm.Engine)
	shardRMap := dbShardRMaps[slaveIndex]

	return BaseRepoImpl{dbShardMap: shardRMap, txShardMap: shardWMap}
}

type BaseRepoImpl struct {
	dbShardMap DBMAP
	txShardMap TXMAP
}

func (r BaseRepoImpl) FindByID(id int) (*User, error) {
	user := new(User)
	var err error

	user.Id = id
	_, err = r.db.Get(user)

	return user, err
}
*/