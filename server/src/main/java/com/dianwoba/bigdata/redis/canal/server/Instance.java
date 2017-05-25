package com.dianwoba.bigdata.redis.canal.server;

import com.dianwoba.bigdata.redis.canal.meta.RedisCanalDTO;
import net.sf.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 19:48
 */
public class Instance {

    private static final Logger LOG = LoggerFactory.getLogger(Instance.class);


    public final static Long DEFAULT_OFFSET = -1L;
    public final static int BATCH_NUM = 50;
    protected volatile String runid;
    protected volatile Connection connection;
    protected volatile Long offset;
    protected volatile LinkedBlockingQueue<SyncData> syncDataQueue = new LinkedBlockingQueue<>();
    protected volatile LinkedBlockingQueue<RedisCanalDTO> redisCanalQueue = new LinkedBlockingQueue<>();
    protected volatile CanalConfig canalConfig;
    protected volatile KafkaUtils kafkaUtils;

    public static Instance createInstance(final CanalConfig canalConfig, final SyncHandlerCallback callback) {

        final Instance instance = new Instance(canalConfig);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                instance.connect();
                callback.start(instance);
            }
        }, "RedisInstance-" + canalConfig.host + '-' + canalConfig.port);
        thread.start();
        return instance;
    }

    public Instance(CanalConfig canalConfig) {
        this.canalConfig = canalConfig;
        this.kafkaUtils = new KafkaUtils(canalConfig.broker);
        this.connection = new Connection(canalConfig.host, canalConfig.port, canalConfig.password);
    }

    public void beginPSync() throws IOException {

        this.initMaster();

//        LOG.info(String.format("start psync host[%s] port[%s] pwd[%s] runid[%s] offset[%s]", this.canalConfig.host, this.canalConfig.port, this.canalConfig.password, this.runid, this.offset));
        System.out.println(String.format("start psync host[%s] port[%s] pwd[%s] runid[%s] offset[%s]", this.canalConfig.host, this.canalConfig.port, this.canalConfig.password, this.runid, this.offset));


        this.connection.sendCommand(Protocol.Command.PSYNC, this.runid, this.offset.toString());
        this.connection.flush();

        class PingTask extends TimerTask {
            private Instance instance;

            public PingTask(Instance instance) {
                this.instance = instance;
            }

            /**
             * The action to be performed by this timer task.
             */
            @Override
            public void run() {

                this.instance.connection.replconf(this.instance.getOffset());
//                LOG.info("\n*********  replconf ack offset:" + this.instance.getOffset() + "  **********\n");
//                System.out.println("\n*********  replconf ack offset:" + this.instance.getOffset() + "  **********\n");
//                this.instance.connection.ping();
            }
        }
//
        Timer timer = new Timer();
        timer.schedule(new PingTask(this), 250, 250);
//

        HandSyncOriDataThread handSyncOriDataThread = new HandSyncOriDataThread(this);
        Thread thread1 = new Thread(handSyncOriDataThread, "HandSyncOriDataThread-1");
        thread1.start();

        HandRedisCanalDataThread handRedisCanalDataThread = new HandRedisCanalDataThread(this);
        Thread thread2 = new Thread(handRedisCanalDataThread, "HandRedisCanalDataThread-1");
        thread2.start();


        BufferedInputStream bis = new BufferedInputStream(this.connection.getInputStream());
        List<Integer> list = new ArrayList<Integer>();
        int b = -1;
        while (true) {
            try {

                int n = 0;
                while ((b = bis.read()) != -1) {
                    list.add(b);
                    n--;
                    if (b == 10 && n < 1) {  //判断每行结束
                        if (list.size() < 2) {
                            list.clear();
                            continue;
                        }
                        byte[] bb = new byte[list.size() - 2];
                        for (int i = 0; i < list.size() - 2; i++) {
                            bb[i] = list.get(i).byteValue();
                        }

                        String s = new String(bb);
                        try {
//                            System.out.println(s);
                            this.syncDataQueue.put(new SyncData(s, System.currentTimeMillis()));  //将解析出来aof记录往队列里添加
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            this.incrOffset(list.size());
                        }

                        if (list.get(0) == 36) {
                            //当前行是$开头，则$后面的数字表示参数的字节数
                            n = Integer.parseInt(s.replace("\r\n", "").substring(1)) + 2;
                        }
                        list.clear();
                    }
                    b = -1;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public Instance connect() {
        this.connection.connect().auth();
        return this;
    }

    //解析AOF线程，往redisCanal队列写数据
    class HandSyncOriDataThread implements Runnable {

        private Instance instance;

        public HandSyncOriDataThread(Instance instance) {
            this.instance = instance;
        }

        public void run() {
            while (true) {
                try {
                    if (this.instance.syncDataQueue.size() > 0) {
                        SyncData syncData = this.instance.syncDataQueue.take();
                        if (syncData.getData().indexOf("*") == 0) {
                            this.instance.syncDataQueue.take(); //剔除$
                            syncData = this.instance.syncDataQueue.take();
                            String key = "", value = "";
                            if (syncData.getData().toLowerCase().equals("set")) {
                                this.instance.syncDataQueue.take();  //剔除$
                                syncData = this.instance.syncDataQueue.take(); //获取key
                                key = syncData.getData();
                                this.instance.syncDataQueue.take();  //剔除$
                                syncData = this.instance.syncDataQueue.take(); //获取value
                                value = syncData.getData();

                            } else if (syncData.getData().toLowerCase().equals("setex")) {
                                this.instance.syncDataQueue.take();  //剔除$
                                syncData = this.instance.syncDataQueue.take(); //获取key
                                key = syncData.getData();
                                this.instance.syncDataQueue.take();  //剔除$
                                this.instance.syncDataQueue.take();  //剔除 过期时间
                                this.instance.syncDataQueue.take();  //剔除$
                                syncData = this.instance.syncDataQueue.take(); //获取value
                                value = syncData.getData();
                            }

                            if (!key.equals("")) {
//                                RedisCanalDTO dto = new RedisCanalDTO(key, EncodeUtils.bytes2Hex(value.getBytes()), syncData.getTimestamp());
                                RedisCanalDTO dto = new RedisCanalDTO(key, value, syncData.getTimestamp());

                                this.instance.redisCanalQueue.put(dto);
//                                LOG.info("redisCanalQueue.size(): " + this.instance.redisCanalQueue.size() + " " + dto.toString());
                                System.out.println("redisCanalQueue.size(): " + this.instance.redisCanalQueue.size() + " " + dto.toString());

                            }

                        }
                    } else {
                        Thread.sleep(500L);
                    }
                } catch (Exception ex) {

                }
            }
        }
    }

    //处理redisCanal队列，往kafka写data
    class HandRedisCanalDataThread implements Runnable {
        private Instance instance;

        public HandRedisCanalDataThread(Instance instance) {
            this.instance = instance;
        }

        public void run() {
            while (true) {
                try {
                    if (this.instance.redisCanalQueue != null && this.instance.redisCanalQueue.size() > 0) {
                        RedisCanalDTO redisCanalDTO = this.instance.redisCanalQueue.take();
                        List<RedisCanalDTO> list = new ArrayList<>();
                        this.instance.redisCanalQueue.drainTo(list, BATCH_NUM);

                        if (list != null && list.size() > 0) {
                            String value = JSONArray.fromObject(list).toString();
                        /*
                        *
                        * 往kafka写数据
                        * */
//                        String topic = "redis.canal.data";

//                            System.out.println("kafka data: " + value);
                            instance.kafkaUtils.produce(instance.canalConfig.topic, redisCanalDTO.getTimestamp().toString(), value);
                        } else {
                            Thread.sleep(500L);
                        }
                    } else {
                        Thread.sleep(500L);
                    }
                } catch (Exception ex) {
                    ;
                }
            }
        }
    }


    public void initMaster() {
        this.connection.sendCommand(Protocol.Command.INFO);
        String info = this.connection.getBulkReply();
        String[] infos = info.split("\r\n");
        this.offset = DEFAULT_OFFSET;
        this.runid = "?";
        for (String i : infos) {
            if (i.toLowerCase().contains("master_repl_offset")) {
                Long index = Long.valueOf(i.split(":")[1]);
                this.setOffset(index);
            }
            if (i.toLowerCase().contains("run_id")) {
                this.runid = i.split(":")[1];
            }
        }
    }

    private Long incrOffset(long incr) {
        synchronized (this) {
            this.offset += incr;
            return this.offset;
        }
    }


    public void shutdown() {
        if (this.connection.isConnected()) {
            this.connection.close();
        }
        if (this.kafkaUtils != null) {
            this.kafkaUtils.closeKafkaProducer();
        }
    }

    private class SyncData {
        private Long timestamp;
        private String data;
        private byte[] bytes;

        public SyncData(String data, Long timestamp) {
            this.data = data;
            this.timestamp = timestamp;
        }

        public SyncData(byte[] bytes, Long timestamp) {
            this.bytes = bytes;
            this.timestamp = timestamp;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(byte[] bytes) {
            this.bytes = bytes;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }


    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getRunid() {
        return runid;
    }

    public void setRunid(String runid) {
        this.runid = runid;
    }
}
