package err

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

type ErrWriter struct {
	errMsgs errArgs
}

/**************************************************************************************************/
/*!
 *  操作オブジェクトの生成
 */
/**************************************************************************************************/
func NewErrWriter(msg ...interface{}) ErrWriter {

	ew := ErrWriter{}

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
func (this ErrWriter) Write(msg ...interface{}) ErrWriter {
	this.errMsgs = append(this.errMsgs, msg...)
	// 呼び出し元
	this = this.addCallerMsg(2)
	return this
}

/**************************************************************************************************/
/*!
 *  エラーか
 *
 *  \return  true / false
 */
/**************************************************************************************************/
func (this ErrWriter) HasErr() bool {
	if len(this.errMsgs) > 0 {
		return true
	}
	return false
}

/**************************************************************************************************/
/*!
 *  エラーを取得する
 *
 *  \return  エラーメッセージ
 */
/**************************************************************************************************/
func (this ErrWriter) Err() errArgs {
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
func (this ErrWriter) addCallerMsg(skip int) ErrWriter {
	// 呼び出し元
	pc, file, line, _ := runtime.Caller(skip)
	callerName := runtime.FuncForPC(pc).Name()

	// 定型文
	addArgs := this.fixedPhrase(file, line, callerName)

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
func (this ErrWriter) fixedPhrase(file string, line int, callerName string) errArgs {
	// 一旦、srcでフィルタする
	splits := strings.Split(file, "/src/")

	// 仮に区切れなくてもエラーにせずそのまま利用する
	fileName := file
	if len(splits) == 2 {
		fileName = splits[1]
	}
	addArgs := errArgs{"(" + callerName + ")", "at", fileName, "line", line}
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
func (this ErrWriter) unshift(v ...interface{}) ErrWriter {
	this.errMsgs = append(v, this.errMsgs...)
	return this
}
