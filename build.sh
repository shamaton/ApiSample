#!/bin/sh
#set -x

#project directory
DIR=`dirname $0`
cd ${DIR}

# go fmt

echo "go fmt..."
for f in `git diff HEAD --name-status | grep "^M\|^A" | cut -f2 | grep '.go$' `;
do
  go fmt ./${f};
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
go install

echo "build package successfully!!"

# recommend server setting
# export PATH=$PATH:$GOPATH/bin
