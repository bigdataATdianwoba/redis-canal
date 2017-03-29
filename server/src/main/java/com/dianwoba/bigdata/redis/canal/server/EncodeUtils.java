package com.dianwoba.bigdata.redis.canal.server;

/**
 * Created by Silas.
 * Date: 2017/3/24
 * Time: 15:37
 */
public class EncodeUtils {

    /**
     * hex字符串转byte数组<br/>
     * 2个hex转为一个byte
     * @param src
     * @return
     */
    public static byte[] hex2Bytes(String src){
        byte[] res = new byte[src.length()/2];
        char[] chs = src.toCharArray();
        for(int i=0,c=0; i<chs.length; i+=2,c++){
            res[c] = (byte) (Integer.parseInt(new String(chs,i,2), 16));
        }

        return res;
    }

    /**
     * byte数组转hex字符串<br/>
     * 一个byte转为2个hex字符
     * @param src
     * @return
     */
    public static String bytes2Hex(byte[] src){
        char[] res = new char[src.length*2];
        final char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        for(int i=0,j=0; i<src.length; i++){
            res[j++] = hexDigits[src[i] >>>4 & 0x0f];
            res[j++] = hexDigits[src[i] & 0x0f];
        }

        return new String(res);
    }

    public static void main(String[] args){
        System.out.println(new String(hex2Bytes("E9BB84")));
    }

}
