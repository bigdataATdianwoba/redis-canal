package com.dianwoba.bigdata.redis.canal.server;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:20
 */
public class HostAndPort implements Serializable {
    private static final long serialVersionUID = -519876229978427751L;
    protected static Logger log = Logger.getLogger(HostAndPort.class.getName());
    public static final String LOCALHOST_STR = getLocalHostQuietly();
    private String host;
    private int port;

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof HostAndPort)) {
            return false;
        } else {
            HostAndPort hp = (HostAndPort)obj;
            String thisHost = convertHost(this.host);
            String hpHost = convertHost(hp.host);
            return this.port == hp.port && thisHost.equals(hpHost);
        }
    }

    public int hashCode() {
        return 31 * convertHost(this.host).hashCode() + this.port;
    }

    public String toString() {
        return this.host + ":" + this.port;
    }

    public static String[] extractParts(String from) {
        int idx = from.lastIndexOf(":");
        String host = idx != -1?from.substring(0, idx):from;
        String port = idx != -1?from.substring(idx + 1):"";
        return new String[]{host, port};
    }

    public static HostAndPort parseString(String from) {
        try {
            String[] ex = extractParts(from);
            String host = ex[0];
            int port = Integer.valueOf(ex[1]).intValue();
            return new HostAndPort(convertHost(host), port);
        } catch (NumberFormatException var4) {
            throw new IllegalArgumentException(var4);
        }
    }

    public static String convertHost(String host) {
        return !host.equals("127.0.0.1") && !host.startsWith("localhost") && !host.equals("0.0.0.0") && !host.startsWith("169.254") && !host.startsWith("::1") && !host.startsWith("0:0:0:0:0:0:0:1")?host:LOCALHOST_STR;
    }

    public static String getLocalHostQuietly() {
        String localAddress;
        try {
            localAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception var2) {
            log.logp(Level.SEVERE, HostAndPort.class.getName(), "getLocalHostQuietly", "cant resolve localhost address", var2);
            localAddress = "localhost";
        }

        return localAddress;
    }
}
