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
	"test_user_create": controller.TestUserCreate,
	"test_user_select": controller.TestUserSelect,
	"test_user_update": controller.TestUserUpdate,

	"test_user_item_create": controller.TestUserItemCreate,
	"test_user_item_delete": controller.TestUserItemDelete,

	"test_user_log_create": controller.TestUserLogCreate,
	"test_user_misc":       controller.TestUserMisc,

	"token_test": controller.TokenTest,
}

// GET
var routerGetConf = map[string]gin.HandlerFunc{
	"shamoto": controller.Shamoto,
}
