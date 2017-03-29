package com.dianwoba.bigdata.redis.canal.server.exception;

import com.dianwoba.bigdata.redis.canal.server.HostAndPort;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:23
 */

public class RedisMovedDataException extends RedisRedirectionException {
    private static final long serialVersionUID = 3878126572474819403L;

    public RedisMovedDataException(String message, HostAndPort targetNode, int slot) {
        super(message, targetNode, slot);
    }

    public RedisMovedDataException(Throwable cause, HostAndPort targetNode, int slot) {
        super(cause, targetNode, slot);
    }

    public RedisMovedDataException(String message, Throwable cause, HostAndPort targetNode, int slot) {
        super(message, cause, targetNode, slot);
    }
}
