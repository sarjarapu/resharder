#!/bin/sh

ps aux | grep -ie mongo | awk '{print $2}' | xargs kill -9

mongod --config ../conf/rs1-1.conf
mongod --config ../conf/rs1-2.conf
mongod --config ../conf/rs1-3.conf
mongod --config ../conf/rs2-1.conf
mongod --config ../conf/rs2-2.conf
mongod --config ../conf/rs2-3.conf
mongod --config ../conf/cfg1.conf
mongod --config ../conf/cfg2.conf
mongod --config ../conf/cfg3.conf
mongod --config ../conf/tgt.conf

sleep 2

mongo --port 28020 << 'EOF'
config={"_id":"rs1","members":[{"_id":0,"host":"localhost:28020", "slaveDelay":0, "hidden":false},{"_id":1,"host":"localhost:28021", "slaveDelay":0, "hidden":false},{"_id":2,"host":"localhost:28022", "slaveDelay":0, "hidden":false}]}
rs.reconfig(config)
EOF

mongo --port 28030 << 'EOF'
config = {"_id":"rs2","members":[{"_id":0,"host":"localhost:28030", "slaveDelay":0, "hidden":false},{"_id":1,"host":"localhost:28031", "slaveDelay":0, "hidden":false},{"_id":2,"host":"localhost:28032", "slaveDelay":0, "hidden":false}]}
rs.reconfig(config)
EOF

eval $(which mongos) --configdb localhost:28040,localhost:28041,localhost:28042 --logpath /usr/local/var/log/mongos.log --fork
