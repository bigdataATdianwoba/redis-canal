package com.dianwoba.bigdata.redis.canal.bootstrap.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.utils.JStormUtils;
import com.dianwoba.bigdata.redis.canal.server.CanalConfig;
import com.dianwoba.bigdata.redis.canal.server.Instance;
import com.dianwoba.bigdata.redis.canal.server.SyncHandlerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Silas.
 * Date: 2017/3/27
 * Time: 15:23
 */
public class CanalBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CanalBolt.class);

    private Instance instance;

    private CanalConfig canalConfig;

    public CanalBolt setConfig(CanalConfig canalConfig) {
        this.canalConfig = canalConfig;
        return this;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.instance = Instance.createInstance(canalConfig, new SyncHandlerCallback() {
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

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void cleanup() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
