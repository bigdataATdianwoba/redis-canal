package com.dianwoba.bigdata.redis.canal.server;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Silas.
 * Date: 2017/3/21
 * Time: 17:09
 */
public final class RedisOutputStream extends FilterOutputStream {
    protected final byte[] buf;
    protected int count;
    private static final int[] sizeTable = new int[]{9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 2147483647};
    private static final byte[] DigitTens = new byte[]{(byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)48, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)49, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)50, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)51, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)52, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)53, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)54, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)55, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)56, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57, (byte)57};
    private static final byte[] DigitOnes = new byte[]{(byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57};
    private static final byte[] digits = new byte[]{(byte)48, (byte)49, (byte)50, (byte)51, (byte)52, (byte)53, (byte)54, (byte)55, (byte)56, (byte)57, (byte)97, (byte)98, (byte)99, (byte)100, (byte)101, (byte)102, (byte)103, (byte)104, (byte)105, (byte)106, (byte)107, (byte)108, (byte)109, (byte)110, (byte)111, (byte)112, (byte)113, (byte)114, (byte)115, (byte)116, (byte)117, (byte)118, (byte)119, (byte)120, (byte)121, (byte)122};

    public RedisOutputStream(OutputStream out) {
        this(out, 8192);
    }

    public RedisOutputStream(OutputStream out, int size) {
        super(out);
        if(size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        } else {
            this.buf = new byte[size];
        }
    }

    private void flushBuffer() throws IOException {
        if(this.count > 0) {
            this.out.write(this.buf, 0, this.count);
            this.count = 0;
        }

    }

    public void write(byte b) throws IOException {
        if(this.count == this.buf.length) {
            this.flushBuffer();
        }

        this.buf[this.count++] = b;
    }

    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if(len >= this.buf.length) {
            this.flushBuffer();
            this.out.write(b, off, len);
        } else {
            if(len >= this.buf.length - this.count) {
                this.flushBuffer();
            }

            System.arraycopy(b, off, this.buf, this.count, len);
            this.count += len;
        }

    }

    public void writeAsciiCrLf(String in) throws IOException {
        int size = in.length();

        for(int i = 0; i != size; ++i) {
            if(this.count == this.buf.length) {
                this.flushBuffer();
            }

            this.buf[this.count++] = (byte)in.charAt(i);
        }

        this.writeCrLf();
    }

    public static boolean isSurrogate(char ch) {
        return ch >= '\ud800' && ch <= '\udfff';
    }

    public static int utf8Length(String str) {
        int strLen = str.length();
        int utfLen = 0;

        for(int i = 0; i != strLen; ++i) {
            char c = str.charAt(i);
            if(c < 128) {
                ++utfLen;
            } else if(c < 2048) {
                utfLen += 2;
            } else if(isSurrogate(c)) {
                ++i;
                utfLen += 4;
            } else {
                utfLen += 3;
            }
        }

        return utfLen;
    }

    public void writeCrLf() throws IOException {
        if(2 >= this.buf.length - this.count) {
            this.flushBuffer();
        }

        this.buf[this.count++] = 13;
        this.buf[this.count++] = 10;
    }

    public void writeUtf8CrLf(String str) throws IOException {
        int strLen = str.length();

        int i;
        char c;
        for(i = 0; i < strLen; ++i) {
            c = str.charAt(i);
            if(c >= 128) {
                break;
            }

            if(this.count == this.buf.length) {
                this.flushBuffer();
            }

            this.buf[this.count++] = (byte)c;
        }

        for(; i < strLen; ++i) {
            c = str.charAt(i);
            if(c < 128) {
                if(this.count == this.buf.length) {
                    this.flushBuffer();
                }

                this.buf[this.count++] = (byte)c;
            } else if(c < 2048) {
                if(2 >= this.buf.length - this.count) {
                    this.flushBuffer();
                }

                this.buf[this.count++] = (byte)(192 | c >> 6);
                this.buf[this.count++] = (byte)(128 | c & 63);
            } else if(isSurrogate(c)) {
                if(4 >= this.buf.length - this.count) {
                    this.flushBuffer();
                }

                int uc = Character.toCodePoint(c, str.charAt(i++));
                this.buf[this.count++] = (byte)(240 | uc >> 18);
                this.buf[this.count++] = (byte)(128 | uc >> 12 & 63);
                this.buf[this.count++] = (byte)(128 | uc >> 6 & 63);
                this.buf[this.count++] = (byte)(128 | uc & 63);
            } else {
                if(3 >= this.buf.length - this.count) {
                    this.flushBuffer();
                }

                this.buf[this.count++] = (byte)(224 | c >> 12);
                this.buf[this.count++] = (byte)(128 | c >> 6 & 63);
                this.buf[this.count++] = (byte)(128 | c & 63);
            }
        }

        this.writeCrLf();
    }

    public void writeIntCrLf(int value) throws IOException {
        if(value < 0) {
            this.write((byte)45);
            value = -value;
        }

        int size;
        for(size = 0; value > sizeTable[size]; ++size) {
            ;
        }

        ++size;
        if(size >= this.buf.length - this.count) {
            this.flushBuffer();
        }

        int q;
        int r;
        int charPos;
        for(charPos = this.count + size; value >= 65536; this.buf[charPos] = DigitTens[r]) {
            q = value / 100;
            r = value - ((q << 6) + (q << 5) + (q << 2));
            value = q;
            --charPos;
            this.buf[charPos] = DigitOnes[r];
            --charPos;
        }

        do {
            q = value * '쳍' >>> 19;
            r = value - ((q << 3) + (q << 1));
            --charPos;
            this.buf[charPos] = digits[r];
            value = q;
        } while(q != 0);

        this.count += size;
        this.writeCrLf();
    }

    public void flush() throws IOException {
        this.flushBuffer();
        this.out.flush();
    }
}
