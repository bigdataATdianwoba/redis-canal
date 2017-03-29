#!/usr/bin/env bash
# 普通jar包启动方式
# java -jar redis-canal-server.jar --name RedisCanal --host localhost --port 6379 --password xxx --broker localhost:9092 --topic redis.canal.data

# jstorm启动方式
# jstorm jar redis-canal-server.jar com.dianwoba.bigdata.redis.canal.bootstrap.jstorm.CanalTopo --name RedisCanal --host localhost --port 6379 --password xxx --broker localhost:9092 --topic redis.canal.data
