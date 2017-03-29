package com.dianwoba.bigdata.redis.canal.server;

import com.dianwoba.bigdata.redis.canal.server.exception.RedisDataException;
import com.dianwoba.bigdata.redis.canal.server.exception.RedisException;

import java.io.UnsupportedEncodingException;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:18
 */
public final class SafeEncoder {
    private SafeEncoder() {
        throw new InstantiationError("Must not instantiate this class");
    }

    public static byte[][] encodeMany(String... strs) {
        byte[][] many = new byte[strs.length][];

        for(int i = 0; i < strs.length; ++i) {
            many[i] = encode(strs[i]);
        }

        return many;
    }

    public static byte[] encode(String str) {
        try {
            if(str == null) {
                throw new RedisDataException("value sent to redis cannot be null");
            } else {
                return str.getBytes("UTF-8");
            }
        } catch (UnsupportedEncodingException var2) {
            throw new RedisException(var2);
        }
    }

    public static String encode(byte[] data) {
        try {
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new RedisException(var2);
        }
    }
}
