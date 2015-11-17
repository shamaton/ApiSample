package main

/**************************************************************************************************/
/*!
 *  route.go
 *
 *  ルート管理ファイル
 *
 */
/**************************************************************************************************/
import (
	"sample/controller"

	"github.com/gin-gonic/gin"
)

// POST
var routerPostConf = map[string]gin.HandlerFunc{
	"test":       controller.Test,
	"token_test": controller.TokenTest,
}

// GET
var routerGetConf = map[string]gin.HandlerFunc{
	"shamoto": controller.Shamoto,
}
