package controller

import (
	"sample/shamoto/model_"

	"github.com/gin-gonic/gin"
	log "github.com/cihub/seelog"
	"net/http"
)

func Shamoto(c *gin.Context) {

	userRepo := model_.NewUserRepo()
	user := userRepo.FindByID(3)

	log.Info(user)


	c.String(http.StatusOK, "hi!!")
}