#!/bin/sh
#set -x

# 引数
EXEC=$1

#project directory
DIR=`dirname $0`
cd ${DIR}

# go fmt

echo "go fmt and imports..."
for f in `git diff HEAD --name-status | grep "^M\|^A" | cut -f2 | grep '.go$' `;
do
  go fmt ./${f};
  goimports -w ./${f}
done

echo ""

# diretory create for build
if [ ! -d "bin" ]; then
  mkdir bin
  echo "make bin directory.."
fi

if [ ! -d "pkg" ]; then
  mkdir pkg
  echo "make pkg directory.."
fi

# build parameter
PJ_DIR=`pwd`
INSTALL_DIR="${PJ_DIR}/src/main"

# build
export GOPATH=$GOPATH:${PJ_DIR}

cd ${INSTALL_DIR}
echo "packaging now..."

# ビルド実行
res=`go install 2>&1`

# ビルドエラー
if [ -n "${res}" ]; then
  # いけてないけど、エラーを改めて出す
  echo "build error found!! please fix!!"
  go install
  exit 1
fi

echo "build package successfully!!"

if [ -n "${EXEC}" ]; then
  cd ${PJ_DIR}
  echo ${DIR}
  pwd
  ./bin/main
fi

# recommend server setting
# export PATH=$PATH:$GOPATH/bin
