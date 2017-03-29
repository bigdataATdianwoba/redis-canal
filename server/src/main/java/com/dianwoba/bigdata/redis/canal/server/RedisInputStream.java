package com.dianwoba.bigdata.redis.canal.server;

import com.dianwoba.bigdata.redis.canal.server.exception.RedisConnectionException;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:09
 */
public class RedisInputStream extends FilterInputStream {
    protected final byte[] buf;
    protected int count;
    protected int limit;

    public RedisInputStream(InputStream in, int size) {
        super(in);
        if(size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        } else {
            this.buf = new byte[size];
        }
    }

    public RedisInputStream(InputStream in) {
        this(in, 8192);
    }

    public InputStream getInputStream(){
        return this.in;
    }


    public byte readByte() throws RedisConnectionException {
        this.ensureFill();
        return this.buf[this.count++];
    }

    public String readLine() {
        StringBuilder sb = new StringBuilder();

        while(true) {
            while(true) {
                this.ensureFill();
                byte reply = this.buf[this.count++];
                if(reply == 13) {
                    this.ensureFill();
                    byte c = this.buf[this.count++];
                    if(c == 10) {
                        String reply1 = sb.toString();
                        if(reply1.length() == 0) {
                            throw new RedisConnectionException("It seems like server has closed the connection.");
                        }

                        return reply1;
                    }

                    sb.append((char)reply);
                    sb.append((char)c);
                } else {
                    sb.append((char)reply);
                }
            }
        }
    }

    public byte[] readLineBytes() {
        this.ensureFill();
        int pos = this.count;
        byte[] buf = this.buf;

        while(pos != this.limit) {
            if(buf[pos++] == 13) {
                if(pos == this.limit) {
                    return this.readLineBytesSlowly();
                }

                if(buf[pos++] == 10) {
                    int N = pos - this.count - 2;
                    byte[] line = new byte[N];
                    System.arraycopy(buf, this.count, line, 0, N);
                    this.count = pos;
                    return line;
                }
            }
        }

        return this.readLineBytesSlowly();
    }

    private byte[] readLineBytesSlowly() {
        ByteArrayOutputStream bout = null;

        while(true) {
            while(true) {
                this.ensureFill();
                byte b = this.buf[this.count++];
                if(b == 13) {
                    this.ensureFill();
                    byte c = this.buf[this.count++];
                    if(c == 10) {
                        return bout == null?new byte[0]:bout.toByteArray();
                    }

                    if(bout == null) {
                        bout = new ByteArrayOutputStream(16);
                    }

                    bout.write(b);
                    bout.write(c);
                } else {
                    if(bout == null) {
                        bout = new ByteArrayOutputStream(16);
                    }

                    bout.write(b);
                }
            }
        }
    }

    public int readIntCrLf() {
        return (int)this.readLongCrLf();
    }

    public long readLongCrLf() {
        byte[] buf = this.buf;
        this.ensureFill();
        boolean isNeg = buf[this.count] == 45;
        if(isNeg) {
            ++this.count;
        }

        long value = 0L;

        while(true) {
            this.ensureFill();
            byte b = buf[this.count++];
            if(b == 13) {
                this.ensureFill();
                if(buf[this.count++] != 10) {
                    throw new RedisConnectionException("Unexpected character!");
                } else {
                    return isNeg?-value:value;
                }
            }

            value = value * 10L + (long)b - 48L;
        }
    }

    public int read(byte[] b, int off, int len) throws RedisConnectionException {
        this.ensureFill();
        int length = Math.min(this.limit - this.count, len);
        System.arraycopy(this.buf, this.count, b, off, length);
        this.count += length;
        return length;
    }

    private void ensureFill() throws RedisConnectionException {
        if(this.count >= this.limit) {
            try {
                this.limit = this.in.read(this.buf);
                this.count = 0;
                if(this.limit == -1) {
                    throw new RedisConnectionException("Unexpected end of stream.");
                }
            } catch (IOException var2) {
                throw new RedisConnectionException(var2);
            }
        }

    }
}
