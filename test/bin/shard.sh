#!/bin/bash

ps aux | grep -ie mongo | awk '{print $2}' | xargs kill -9

rm -rf ../data/*

mkdir -p ../data/rs1-1
mkdir ../data/rs1-2
mkdir ../data/rs1-3
mkdir ../data/rs2-1
mkdir ../data/rs2-2
mkdir ../data/rs2-3
mkdir ../data/cfg1
mkdir ../data/cfg2
mkdir ../data/cfg3
mkdir ../data/tgt
mkdir ../data/mongos

mongod --config ../conf/rs1-1.conf
mongod --config ../conf/rs1-2.conf
mongod --config ../conf/rs1-3.conf
mongod --config ../conf/rs2-1.conf
mongod --config ../conf/rs2-2.conf
mongod --config ../conf/rs2-3.conf
mongod --config ../conf/cfg1.conf
mongod --config ../conf/cfg2.conf
mongod --config ../conf/cfg3.conf
# mongod --config ../conf/tgt.conf

mongo --port 28020 << 'EOF'
config = {"_id":"rs1","members":[{"_id":0,"host":"localhost:28020"},{"_id":1,"host":"localhost:28021"},{"_id":2,"host":"localhost:28022"}]}
rs.initiate(config)
EOF

mongo --port 28030 << 'EOF'
config = {"_id":"rs2","members":[{"_id":0,"host":"localhost:28030"},{"_id":1,"host":"localhost:28031"},{"_id":2,"host":"localhost:28032"}]}
rs.initiate(config)
EOF

mongo --port 28040 << 'EOF'
config = {"_id":"cfg","members":[{"_id":0,"host":"localhost:28040"},{"_id":1,"host":"localhost:28041"},{"_id":2,"host":"localhost:28042"}]}
rs.initiate(config)
EOF

echo "Waiting for Shards to initialize..."

sleep 30

eval $(which mongos) --port 28010 --configdb cfg/localhost:28040,localhost:28041,localhost:28042 --logpath ../data/mongos/mongos.log --fork

mongo --port 28010 << 'EOF'
use admin
sh.addShard("rs1/localhost:28020,localhost:28021,localhost:28022")
sh.addShard("rs2/localhost:28030,localhost:28031,localhost:28032")
sh.enableSharding("test")
sh.shardCollection("test.grades",{student_id:1})
EOF

c=1
while [ $c -le 300 ]
do
	mongoimport --port 28010 --db test --collection grades --jsonArray --file ../import/records.json
	(( c++ ))
done
