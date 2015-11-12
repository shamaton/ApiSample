package controller

import (
	"sample/model"

	log "github.com/cihub/seelog"
	"github.com/gin-gonic/gin"
	"net/http"
)

func Shamoto(c *gin.Context) {

	userRepo := model.NewUserRepo()
	user, _ := userRepo.FindByID(c, 3)

	log.Info(user)

	c.String(http.StatusOK, "hi!!")
}
