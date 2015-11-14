package controller

import (
	"sample/model"

	"net/http"
	"sample/DBI"

	"time"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
)

func Shamoto(c *gin.Context) {
	defer DBI.RollBack(c)

	userRepo := model.NewUserRepo()
	user, _ := userRepo.FindByID(c, 3)

	// unixtimeに変換
	log.Debug(time.Now())
	log.Debug(user.CreatedAt.Unix())
	log.Debug(user.UpdatedAt.Location())

	log.Info(user)

	c.String(http.StatusOK, "hi!!")
}
