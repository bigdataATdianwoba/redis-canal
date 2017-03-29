package com.dianwoba.bigdata.redis.canal.server;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * Created by Silas.
 * Date: 2017/3/24
 * Time: 16:08
 */
public class CanalConfig implements Serializable {


    private static final long serialVersionUID = 1L;

    // app name
    @Parameter(names = {"-n", "--name"}, description = "app name")
    public String name = "ReidsCanal";

    // host
    @Parameter(names = {"-h", "--host"}, description = "redis host")
    public String host = "127.0.0.1";

    // port
    @Parameter(names = {"-p", "--port"}, description = "redis port")
    public Integer port = 6379;

    // password
    @Parameter(names = {"-pwd", "--password"}, description = "redis password")
    public String password = "";

    // filter
    @Parameter(names = {"-f", "--filter"}, description = "redis filter")
    public String filter = "";

    // broker
    @Parameter(names = {"-b", "--broker"}, description = "kafka broker")
    public String broker = "";

    // topic
    @Parameter(names = {"-t", "--topic"}, description = "kafka topic")
    public String topic = "";

    public CanalConfig() {
    }

}
