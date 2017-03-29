package com.dianwoba.bigdata.redis.canal.bootstrap.jar;

import com.beust.jcommander.JCommander;
import com.dianwoba.bigdata.redis.canal.server.CanalConfig;
import com.dianwoba.bigdata.redis.canal.server.Instance;
import com.dianwoba.bigdata.redis.canal.server.SyncHandlerCallback;

import java.io.IOException;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:35
 */
public class Start {


    // java -jar redis-canal-server.jar --name RedisCanal --host localhost --port 6379 --password xxx --broker localhost:9092 --topic redis.canal.data
    public static void main(String[] args) {

        CanalConfig canalConfig = new CanalConfig();
        new JCommander(canalConfig, args);

        Instance.createInstance(canalConfig, new SyncHandlerCallback() {
            @Override
            public void start(Instance instance) {
                try {
                    instance.beginPSync();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    instance.shutdown();
                }
            }
        });
    }

}
