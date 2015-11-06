package cache
/*
import (
	"github.com/gin-gonic/gin"
	"github.com/garyburd/redigo/redis"
)


type TxError struct {
	Err   error
	TxErr error
}
func (t TxError) Error() string {
	return fmt.Sprintf("Transaction Error: Err(%v) TxErr(%v)", t.Err, t.TxErr)
}

func Tx(db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return TxError{ err, nil }
	}

	if err := fn(tx); err != nil {
		return TxError{ err, tx.Rollback() }
	}
	if err := tx.Commit(); err != nil {
		return TxError{ nil, err }
	}

	return nil
}

func Set(c *gin.Context) {

}


type Cache interface {
	FindByID(int) (*User, error)
}

func NewUserRepo(c *gin.Context) UserRepo {
	tx, _ := DBI.GetDBSession(c)
	db, _ := DBI.GetDBConnection(c, "user")

	return UserRepoImpl{db: db, tx: tx}
}

type CacheImpl struct {
	conn redis.Pool
}

func (r UserRepoImpl) FindByID(id int) (*User, error) {
	user := new(User)
	var err error

	user.Id = id
	_, err = r.db.Get(user)

	return user, err
}
*/