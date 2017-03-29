package com.dianwoba.bigdata.redis.canal.server;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:13
 */
public class IOUtils {
    private IOUtils() {
    }

    public static void closeQuietly(Socket sock) {
        if(sock != null) {
            try {
                sock.close();
            } catch (IOException var2) {
                ;
            }
        }

    }
}
