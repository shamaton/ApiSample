#!/bin/sh
#set -x

#project directory
DIR=`dirname $0`
cd ${DIR}

mysql -ugame -pgame game_master < master.sql

mysql -ugame -pgame game_shard_1 < shard.sql
mysql -ugame -pgame game_shard_2 < shard.sql
