package controller

import (

	"sample/model2"
	"github.com/gin-gonic/gin"
	log "github.com/cihub/seelog"
	"net/http"
)

func Test2(c *gin.Context) {

	userRepo := model2.NewUserRepo(c)
	user, err := userRepo.FindByID(3)

	log.Info(user)

	if err != nil {
		log.Error("error : ", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
	}

	c.JSON(http.StatusOK, &user)
}