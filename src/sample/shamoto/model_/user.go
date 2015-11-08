package model_

import (
	builder "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
)

type User struct {
	Id int32
	Name  string
	Score int32
	//Hoge int32   //`db:"score, [primarykey, autoincrement]"` 変数名とカラム名が異なる場合JSON的に書ける
}

// user
/////////////////////////////
type UserRepo interface {
	FindByID(int) *User
}

func NewUserRepo() UserRepo {
	b := &base{}
	return UserRepoImpl{b}
}

type UserRepoImpl struct {
	*base
}
func (r UserRepoImpl) FindByID(id int) *User {
	var user = new(User)
	sb := builder.Select("id, name, score").From("user").Where("id = ?", id)
	r.Find(user, sb)
	return user
}


