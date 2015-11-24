package error

/**************************************************************************************************/
/*!
 *  error.go
 *
 *  エラーメッセージをスタックするモジュール
 *
 */
/**************************************************************************************************/
import (
	"runtime"
	"strings"
)

type errArgs []interface{}

type errWriter struct {
	errMsgs []interface{}
}

/**************************************************************************************************/
/*!
 *  操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewErrWriter(msg ...interface{}) errWriter {

	ew := errWriter{}

	// msgがある場合追加しておく
	if len(msg) > 0 {
		ew.errMsgs = append(ew.errMsgs, msg...)
		ew = ew.addCallerMsg(2)
	}

	return ew
}

/**************************************************************************************************/
/*!
 *  エラーメッセージを取得する
 *
 *  \param   msg : エラーメッセージ
 *  \return  更新後のエラースタック
 */
/**************************************************************************************************/
func (this errWriter) Write(msg ...interface{}) errWriter {
	this.errMsgs = append(this.errMsgs, msg...)
	// 呼び出し元
	this = this.addCallerMsg(2)
	return this
}

/**************************************************************************************************/
/*!
 *  エラーを取得する
 *
 *  \return  エラーメッセージ
 */
/**************************************************************************************************/
func (this errWriter) Err() errArgs {
	if len(this.errMsgs) < 1 {
		return nil
	}
	return this.errMsgs
}

/**************************************************************************************************/
/*!
 *  呼び出し元の情報をエラーに追加する
 *
 *  \param   level : callerに渡すskip値
 *  \return  更新後のエラースタック
 */
/**************************************************************************************************/
func (this errWriter) addCallerMsg(skip int) errWriter {
	// 呼び出し元
	_, file, line, _ := runtime.Caller(skip)

	// 定型文
	addArgs := this.fixedPhrase(file, line)

	// 追加
	this.errMsgs = append(this.errMsgs, addArgs...)

	return this
}

/**************************************************************************************************/
/*!
 *  callerの情報を整形する
 *
 *  \param   msg : エラーメッセージ
 *  \return  メッセージ配列
 */
/**************************************************************************************************/
func (this errWriter) fixedPhrase(file string, line int) errArgs {
	// 一旦、srcでフィルタする
	splits := strings.Split(file, "/src/")

	// 仮に区切れなくてもエラーにせずそのまま利用する
	fileName := file
	if len(splits) == 2 {
		fileName = splits[1]
	}
	addArgs := errArgs{"at", fileName, "line", line}
	return addArgs
}

/**************************************************************************************************/
/*!
 *  unshiftする
 *
 *  \param   v : エラーメッセージ
 *  \return  更新後のエラースタック
 */
/**************************************************************************************************/
func (this errWriter) unshift(v ...interface{}) errWriter {
	this.errMsgs = append(v, this.errMsgs...)
	return this
}
