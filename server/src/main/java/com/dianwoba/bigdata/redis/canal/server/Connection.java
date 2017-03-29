package com.dianwoba.bigdata.redis.canal.server;

import com.dianwoba.bigdata.redis.canal.server.exception.RedisConnectionException;
import com.dianwoba.bigdata.redis.canal.server.exception.RedisDataException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 16:47
 */
public class Connection implements Closeable {

    private static final byte[][] EMPTY_ARGS = new byte[0][];
    private String host = "localhost";
    private int port = 6379;
    private String pwd = "";
    private Socket socket;
    private RedisOutputStream outputStream;
    private RedisInputStream inputStream;
    private int pipelinedCommands = 0;
    private int connectionTimeout = 10000;
    private int soTimeout = 0;
    private boolean broken = false;
    private boolean ssl;
    private SSLSocketFactory sslSocketFactory;
    private SSLParameters sslParameters;
    private HostnameVerifier hostnameVerifier;
    private Long offset = -1L;

    public Connection() {
    }

    public Connection(String host) {
        this.host = host;
    }

    public Connection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Connection(String host, int port, String pwd) {
        this.host = host;
        this.port = port;
        this.pwd = pwd;
    }


    public Connection(String host, int port, boolean ssl) {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
    }

    public Connection(String host, int port, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Socket getSocket() {
        return this.socket;
    }

    public int getConnectionTimeout() {
        return this.connectionTimeout;
    }

    public int getSoTimeout() {
        return this.soTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setTimeoutInfinite() {
        try {
            if (!this.isConnected()) {
                this.connect();
            }

            this.socket.setSoTimeout(0);
        } catch (SocketException var2) {
            this.broken = true;
            throw new RedisConnectionException(var2);
        }
    }

    public RedisInputStream getInputStream() {
        return inputStream;
    }

    public void rollbackTimeout() {
        try {
            this.socket.setSoTimeout(this.soTimeout);
        } catch (SocketException var2) {
            this.broken = true;
            throw new RedisConnectionException(var2);
        }
    }

    public Connection sendCommand(Protocol.Command cmd, String... args) {
        byte[][] bargs = new byte[args.length][];

        for (int i = 0; i < args.length; ++i) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }

        return this.sendCommand(cmd, bargs);
    }

    public Connection sendCommand(Protocol.Command cmd) {
        return this.sendCommand(cmd, EMPTY_ARGS);
    }

    protected Connection sendCommand(Protocol.Command cmd, byte[]... args) {
        try {
            this.connect();
            Protocol.sendCommand(this.outputStream, cmd, args);
            ++this.pipelinedCommands;
            return this;
        } catch (RedisConnectionException var6) {
            RedisConnectionException ex = var6;

            try {
                String errorMessage = Protocol.readErrorLineIfPossible(this.inputStream);
                if (errorMessage != null && errorMessage.length() > 0) {
                    ex = new RedisConnectionException(errorMessage, ex.getCause());
                }
            } catch (Exception var5) {
                ;
            }

            this.broken = true;
            throw ex;
        }
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Connection connect() {
        if (!this.isConnected()) {
            try {
                this.socket = new Socket();
                this.socket.setReuseAddress(true);
                this.socket.setKeepAlive(true);
                this.socket.setTcpNoDelay(true);
                this.socket.setSoLinger(true, 0);
                this.socket.connect(new InetSocketAddress(this.host, this.port), this.connectionTimeout);
                this.socket.setSoTimeout(this.soTimeout);
                if (this.ssl) {
                    if (null == this.sslSocketFactory) {
                        this.sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                    }

                    this.socket = (SSLSocket) this.sslSocketFactory.createSocket(this.socket, this.host, this.port, true);
                    if (null != this.sslParameters) {
                        ((SSLSocket) this.socket).setSSLParameters(this.sslParameters);
                    }

                    if (null != this.hostnameVerifier && !this.hostnameVerifier.verify(this.host, ((SSLSocket) this.socket).getSession())) {
                        String ex = String.format("The connection to \'%s\' failed ssl/tls hostname verification.", new Object[]{this.host});
                        throw new RedisConnectionException(ex);
                    }
                }

                this.outputStream = new RedisOutputStream(this.socket.getOutputStream());
                this.inputStream = new RedisInputStream(this.socket.getInputStream());
            } catch (IOException var2) {
                this.broken = true;
                throw new RedisConnectionException(var2);
            }
        }

        return this;

    }

    public void close() {
        this.disconnect();
    }

    public void disconnect() {
        if (this.isConnected()) {
            try {
                this.outputStream.flush();
                this.socket.close();
            } catch (IOException var5) {
                this.broken = true;
                throw new RedisConnectionException(var5);
            } finally {
                IOUtils.closeQuietly(this.socket);
            }
        }

    }

    public boolean isConnected() {
        return this.socket != null && this.socket.isBound() && !this.socket.isClosed() && this.socket.isConnected() && !this.socket.isInputShutdown() && !this.socket.isOutputShutdown();
    }

    public String getStatusCodeReply() {
        this.flush();
        --this.pipelinedCommands;
        byte[] resp = (byte[]) ((byte[]) this.readProtocolWithCheckingBroken());
        return null == resp ? null : SafeEncoder.encode(resp);
    }

    public String getBulkReply() {
        byte[] result = this.getBinaryBulkReply();
        return null != result ? SafeEncoder.encode(result) : null;
    }

    public byte[] getBinaryBulkReply() {
        this.flush();
        --this.pipelinedCommands;
        return (byte[]) ((byte[]) this.readProtocolWithCheckingBroken());
    }

    public Long getIntegerReply() {
        this.flush();
        --this.pipelinedCommands;
        return (Long) this.readProtocolWithCheckingBroken();
    }

   /* public List<String> getMultiBulkReply() {
        return (List) BuilderFactory.STRING_LIST.build(this.getBinaryMultiBulkReply());
    }*/

    public List<byte[]> getBinaryMultiBulkReply() {
        this.flush();
        --this.pipelinedCommands;
        return (List) this.readProtocolWithCheckingBroken();
    }

    public void resetPipelinedCount() {
        this.pipelinedCommands = 0;
    }

    public List<Object> getRawObjectMultiBulkReply() {
        return (List) this.readProtocolWithCheckingBroken();
    }

    public List<Object> getObjectMultiBulkReply() {
        this.flush();
        --this.pipelinedCommands;
        return this.getRawObjectMultiBulkReply();
    }

    public List<Long> getIntegerMultiBulkReply() {
        this.flush();
        --this.pipelinedCommands;
        return (List) this.readProtocolWithCheckingBroken();
    }

    public List<Object> getAll() {
        return this.getAll(0);
    }

    public List<Object> getAll(int except) {
        ArrayList all = new ArrayList();
        this.flush();

        for (; this.pipelinedCommands > except; --this.pipelinedCommands) {
            try {
                all.add(this.readProtocolWithCheckingBroken());
            } catch (RedisDataException var4) {
                all.add(var4);
            }
        }

        return all;
    }

    public Object getOne() {
        this.flush();
        --this.pipelinedCommands;
        return this.readProtocolWithCheckingBroken();
    }

    public boolean isBroken() {
        return this.broken;
    }

    protected void flush() {
        try {
            this.outputStream.flush();
        } catch (IOException var2) {
            this.broken = true;
            throw new RedisConnectionException(var2);
        }
    }


    protected Object readProtocolWithCheckingBroken() {
        try {
            return Protocol.read(this.inputStream);
        } catch (RedisConnectionException var2) {
            this.broken = true;
            throw var2;
        }
    }

    public Connection auth() {
        if (this.pwd != null && !"".equals(this.pwd)) {
            this.sendCommand(Protocol.Command.AUTH, this.pwd);
            this.getStatusCodeReply();
        }

        return this;
    }

    public Connection ping() {
        this.sendCommand(Protocol.Command.PING);
//        System.out.println("ping:" + this.getStatusCodeReply());
        return this;
    }

    public Connection replconf(Long offset) {
        this.setOffset(offset);
        this.sendCommand(Protocol.Command.REPLCONF, "ACK", this.offset.toString());

        return this;
    }
}
