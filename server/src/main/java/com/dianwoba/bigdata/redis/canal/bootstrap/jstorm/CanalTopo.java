package com.dianwoba.bigdata.redis.canal.bootstrap.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.beust.jcommander.JCommander;
import com.dianwoba.bigdata.redis.canal.server.CanalConfig;

/**
 * Created by Silas.
 * Date: 2017/3/27
 * Time: 15:22
 */
public class CanalTopo {

    public static String REDIS_CANAL_BOLT = "REDIS_CANAL_BOLT";

    private StormTopology buildTopo(CanalConfig canalConfig) {
        TopologyBuilder builder = new TopologyBuilder();

        //bolt
        builder.setBolt(REDIS_CANAL_BOLT,
                new CanalBolt().setConfig(canalConfig),
                1);
        return builder.createTopology();
    }


    // jstorm jar redis-canal-server.jar com.dianwoba.bigdata.redis.canal.bootstrap.jstorm.CanalTopo --name RedisCanal --host localhost --port 6379 --password xxx --broker localhost:9092 --topic redis.canal.data
    public static void main(String[] args) {

        if (args == null || args.length == 0) {
            System.out.println("启动失败，请指定所需的参数");
            return;
        }

        CanalConfig canalConfig = new CanalConfig();
        new JCommander(canalConfig, args);

        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 5000);
        config.setNumWorkers(1);
        config.setNumAckers(0);

        try {
            StormSubmitter.submitTopologyWithProgressBar(canalConfig.name, config, new CanalTopo().buildTopo(canalConfig));
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }

    }

}
