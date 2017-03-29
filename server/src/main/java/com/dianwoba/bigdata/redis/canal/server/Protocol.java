package com.dianwoba.bigdata.redis.canal.server;

import com.dianwoba.bigdata.redis.canal.server.exception.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 16:45
 */

public final class Protocol {
    private static final String ASK_RESPONSE = "ASK";
    private static final String MOVED_RESPONSE = "MOVED";
    private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
    private static final String BUSY_RESPONSE = "BUSY";
    private static final String NOSCRIPT_RESPONSE = "NOSCRIPT";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_SENTINEL_PORT = 26379;
    public static final int DEFAULT_TIMEOUT = 2000;
    public static final int DEFAULT_DATABASE = 0;
    public static final String CHARSET = "UTF-8";
    public static final byte DOLLAR_BYTE = 36;
    public static final byte ASTERISK_BYTE = 42;
    public static final byte PLUS_BYTE = 43;
    public static final byte MINUS_BYTE = 45;
    public static final byte COLON_BYTE = 58;
    public static final String SENTINEL_MASTERS = "masters";
    public static final String SENTINEL_GET_MASTER_ADDR_BY_NAME = "get-master-addr-by-name";
    public static final String SENTINEL_RESET = "reset";
    public static final String SENTINEL_SLAVES = "slaves";
    public static final String SENTINEL_FAILOVER = "failover";
    public static final String SENTINEL_MONITOR = "monitor";
    public static final String SENTINEL_REMOVE = "remove";
    public static final String SENTINEL_SET = "set";
    public static final String CLUSTER_NODES = "nodes";
    public static final String CLUSTER_MEET = "meet";
    public static final String CLUSTER_RESET = "reset";
    public static final String CLUSTER_ADDSLOTS = "addslots";
    public static final String CLUSTER_DELSLOTS = "delslots";
    public static final String CLUSTER_INFO = "info";
    public static final String CLUSTER_GETKEYSINSLOT = "getkeysinslot";
    public static final String CLUSTER_SETSLOT = "setslot";
    public static final String CLUSTER_SETSLOT_NODE = "node";
    public static final String CLUSTER_SETSLOT_MIGRATING = "migrating";
    public static final String CLUSTER_SETSLOT_IMPORTING = "importing";
    public static final String CLUSTER_SETSLOT_STABLE = "stable";
    public static final String CLUSTER_FORGET = "forget";
    public static final String CLUSTER_FLUSHSLOT = "flushslots";
    public static final String CLUSTER_KEYSLOT = "keyslot";
    public static final String CLUSTER_COUNTKEYINSLOT = "countkeysinslot";
    public static final String CLUSTER_SAVECONFIG = "saveconfig";
    public static final String CLUSTER_REPLICATE = "replicate";
    public static final String CLUSTER_SLAVES = "slaves";
    public static final String CLUSTER_FAILOVER = "failover";
    public static final String CLUSTER_SLOTS = "slots";
    public static final String PUBSUB_CHANNELS = "channels";
    public static final String PUBSUB_NUMSUB = "numsub";
    public static final String PUBSUB_NUM_PAT = "numpat";
    public static final byte[] BYTES_TRUE = toByteArray(1);
    public static final byte[] BYTES_FALSE = toByteArray(0);

    private Protocol() {
    }

    public static void sendCommand(RedisOutputStream os, Protocol.Command command, byte[]... args) {
        sendCommand(os, command.raw, args);
    }

    private static void sendCommand(RedisOutputStream os, byte[] command, byte[]... args) {
        try {
            os.write((byte) 42);
            os.writeIntCrLf(args.length + 1);
            os.write((byte) 36);
            os.writeIntCrLf(command.length);
            os.write(command);
            os.writeCrLf();
            byte[][] e = args;
            int var4 = args.length;

            for (int var5 = 0; var5 < var4; ++var5) {
                byte[] arg = e[var5];
                os.write((byte) 36);
                os.writeIntCrLf(arg.length);
                os.write(arg);
                os.writeCrLf();
            }

        } catch (IOException var7) {
            throw new RedisConnectionException(var7);
        }
    }

    private static void processError(RedisInputStream is) {
        String message = is.readLine();
        String[] askInfo;
        if (message.startsWith("MOVED")) {
            askInfo = parseTargetHostAndSlot(message);
            throw new RedisMovedDataException(message, new HostAndPort(askInfo[1], Integer.valueOf(askInfo[2]).intValue()), Integer.valueOf(askInfo[0]).intValue());
        } else if (message.startsWith("ASK")) {
            askInfo = parseTargetHostAndSlot(message);
            throw new RedisAskDataException(message, new HostAndPort(askInfo[1], Integer.valueOf(askInfo[2]).intValue()), Integer.valueOf(askInfo[0]).intValue());
        } else if (message.startsWith("CLUSTERDOWN")) {
            throw new RedisClusterException(message);
        } else if (message.startsWith("BUSY")) {
            throw new RedisBusyException(message);
        } else if (message.startsWith("NOSCRIPT")) {
            throw new RedisNoScriptException(message);
        } else {
            throw new RedisDataException(message);
        }
    }

    public static String readErrorLineIfPossible(RedisInputStream is) {
        byte b = is.readByte();
        return b != 45 ? null : is.readLine();
    }

    private static String[] parseTargetHostAndSlot(String clusterRedirectResponse) {
        String[] response = new String[3];
        String[] messageInfo = clusterRedirectResponse.split(" ");
        String[] targetHostAndPort = HostAndPort.extractParts(messageInfo[2]);
        response[0] = messageInfo[1];
        response[1] = targetHostAndPort[0];
        response[2] = targetHostAndPort[1];
        return response;
    }

    private static Object process(RedisInputStream is) {
        byte b = is.readByte();

//        System.out.println("b: " + b);

        if (b == 10) {   //"\n"
            return null;
        } else if (b == 43) {   //"+"
            return processStatusCodeReply(is);
        } else if (b == 36) {   //"$"
            return processBulkReply(is);
        } else if (b == 42) {   //"*"
            return processMultiBulkReply(is);
        } else if (b == 58) {   //":"
            return processInteger(is);
        } else if (b == 45) {   //"-"
            processError(is);
            return null;
        } else {
            throw new RedisConnectionException("Unknown reply: " + (char) b);
        }
    }

    public static byte[] processStatusCodeReply(RedisInputStream is) {
        return is.readLineBytes();
    }

    public static byte[] processBulkReply(RedisInputStream is) {
        int len = is.readIntCrLf();
        if (len == -1) {
            return null;
        } else {
            byte[] read = new byte[len];

            int size;
            for (int offset = 0; offset < len; offset += size) {
                size = is.read(read, offset, len - offset);
                if (size == -1) {
                    throw new RedisConnectionException("It seems like server has closed the connection.");
                }
            }

            is.readByte();
            is.readByte();
            return read;
        }
    }

    public static Long processInteger(RedisInputStream is) {
        return Long.valueOf(is.readLongCrLf());
    }

    public static List<Object> processMultiBulkReply(RedisInputStream is) {
        int num = is.readIntCrLf();
        if (num == -1) {
            return null;
        } else {
            ArrayList ret = new ArrayList(num);

            for (int i = 0; i < num; ++i) {
                try {
                    ret.add(process(is));
                } catch (RedisDataException var5) {
                    ret.add(var5);
                }
            }

            return ret;
        }
    }

    public static Object read(RedisInputStream is) {
        return process(is);
    }

    public static final byte[] toByteArray(boolean value) {
        return value ? BYTES_TRUE : BYTES_FALSE;
    }

    public static final byte[] toByteArray(int value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    public static final byte[] toByteArray(long value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    public static final byte[] toByteArray(double value) {
        return Double.isInfinite(value) ? (value == 1.0D / 0.0 ? "+inf".getBytes() : "-inf".getBytes()) : SafeEncoder.encode(String.valueOf(value));
    }

    public static enum Keyword {
        AGGREGATE,
        ALPHA,
        ASC,
        BY,
        DESC,
        GET,
        LIMIT,
        MESSAGE,
        NO,
        NOSORT,
        PMESSAGE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        OK,
        ONE,
        QUEUED,
        SET,
        STORE,
        SUBSCRIBE,
        UNSUBSCRIBE,
        WEIGHTS,
        WITHSCORES,
        RESETSTAT,
        RESET,
        FLUSH,
        EXISTS,
        LOAD,
        KILL,
        LEN,
        REFCOUNT,
        ENCODING,
        IDLETIME,
        AND,
        OR,
        XOR,
        NOT,
        GETNAME,
        SETNAME,
        LIST,
        MATCH,
        COUNT,
        PING,
        PONG;

        public final byte[] raw;

        private Keyword() {
            this.raw = SafeEncoder.encode(this.name().toLowerCase(Locale.ENGLISH));
        }
    }

    public static enum Command {
        PING,
        SET,
        GET,
        QUIT,
        EXISTS,
        DEL,
        TYPE,
        FLUSHDB,
        KEYS,
        RANDOMKEY,
        RENAME,
        RENAMENX,
        RENAMEX,
        DBSIZE,
        EXPIRE,
        EXPIREAT,
        TTL,
        SELECT,
        MOVE,
        FLUSHALL,
        GETSET,
        MGET,
        SETNX,
        SETEX,
        MSET,
        MSETNX,
        DECRBY,
        DECR,
        INCRBY,
        INCR,
        APPEND,
        SUBSTR,
        HSET,
        HGET,
        HSETNX,
        HMSET,
        HMGET,
        HINCRBY,
        HEXISTS,
        HDEL,
        HLEN,
        HKEYS,
        HVALS,
        HGETALL,
        RPUSH,
        LPUSH,
        LLEN,
        LRANGE,
        LTRIM,
        LINDEX,
        LSET,
        LREM,
        LPOP,
        RPOP,
        RPOPLPUSH,
        SADD,
        SMEMBERS,
        SREM,
        SPOP,
        SMOVE,
        SCARD,
        SISMEMBER,
        SINTER,
        SINTERSTORE,
        SUNION,
        SUNIONSTORE,
        SDIFF,
        SDIFFSTORE,
        SRANDMEMBER,
        ZADD,
        ZRANGE,
        ZREM,
        ZINCRBY,
        ZRANK,
        ZREVRANK,
        ZREVRANGE,
        ZCARD,
        ZSCORE,
        MULTI,
        DISCARD,
        EXEC,
        WATCH,
        UNWATCH,
        SORT,
        BLPOP,
        BRPOP,
        AUTH,
        SUBSCRIBE,
        PUBLISH,
        UNSUBSCRIBE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        PUBSUB,
        ZCOUNT,
        ZRANGEBYSCORE,
        ZREVRANGEBYSCORE,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZUNIONSTORE,
        ZINTERSTORE,
        ZLEXCOUNT,
        ZRANGEBYLEX,
        ZREVRANGEBYLEX,
        ZREMRANGEBYLEX,
        SAVE,
        BGSAVE,
        BGREWRITEAOF,
        LASTSAVE,
        SHUTDOWN,
        INFO,
        MONITOR,
        SLAVEOF,
        CONFIG,
        STRLEN,
        SYNC,
        PSYNC,
        LPUSHX,
        PERSIST,
        RPUSHX,
        ECHO,
        LINSERT,
        DEBUG,
        BRPOPLPUSH,
        SETBIT,
        GETBIT,
        BITPOS,
        SETRANGE,
        GETRANGE,
        EVAL,
        EVALSHA,
        SCRIPT,
        SLOWLOG,
        OBJECT,
        BITCOUNT,
        BITOP,
        SENTINEL,
        DUMP,
        RESTORE,
        PEXPIRE,
        PEXPIREAT,
        PTTL,
        INCRBYFLOAT,
        PSETEX,
        CLIENT,
        TIME,
        MIGRATE,
        HINCRBYFLOAT,
        SCAN,
        HSCAN,
        SSCAN,
        ZSCAN,
        WAIT,
        CLUSTER,
        ASKING,
        PFADD,
        PFCOUNT,
        PFMERGE,
        READONLY,
        GEOADD,
        GEODIST,
        GEOHASH,
        GEOPOS,
        GEORADIUS,
        GEORADIUSBYMEMBER,
        BITFIELD,
        REPLCONF;

        public final byte[] raw = SafeEncoder.encode(this.name());

        private Command() {
        }
    }
}
