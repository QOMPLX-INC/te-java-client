/*
 * Copyright 2015-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
 *
 */
package com.qomplx.mdtsdb.client.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonParser;
import com.google.gson.JsonNull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.*;
import java.nio.channels.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class ErlangTermToBinWrp
{
    public static final byte FORMAT_VERSION = (byte)131;

    public static final byte NEW_FLOAT_EXT = (byte)70;      // [Float64:IEEE float]
    public static final byte BIT_BINARY_EXT = (byte)77;     // [UInt32:Len, UInt8:Bits, Len:Data]
    public static final byte SMALL_INTEGER_EXT = (byte)97;  // [UInt8:Int]
    public static final byte INTEGER_EXT = (byte)98;        // [Int32:Int]
    public static final byte FLOAT_EXT = (byte)99;          // [31:Float String] Float in string format (formatted "%.20e", sscanf "%lf"). Superseded by NEW_FLOAT_EXT
    public static final byte ATOM_EXT = (byte)100;          // 100 [UInt16:Len, Len:AtomName] max Len is 255
    public static final byte REFERENCE_EXT = (byte)101;     // 101 [atom:Node, UInt32:ID, UInt8:Creation]
    public static final byte PORT_EXT = (byte)102;          // [atom:Node, UInt32:ID, UInt8:Creation]
    public static final byte PID_EXT = (byte)103;           // [atom:Node, UInt32:ID, UInt32:Serial, UInt8:Creation]
    public static final byte SMALL_TUPLE_EXT = (byte)104;   // [UInt8:Arity, N:Elements]
    public static final byte LARGE_TUPLE_EXT = (byte)105;   // [UInt32:Arity, N:Elements]
    public static final byte NIL_EXT = (byte)106;           // empty list
    public static final byte STRING_EXT = (byte)107;        // [UInt32:Len, Len:Characters]
    public static final byte LIST_EXT = (byte)108;          // [UInt32:Len, Elements, Tail]
    public static final byte BINARY_EXT = (byte)109;        // [UInt32:Len, Len:Data]
    public static final byte SMALL_BIG_EXT = (byte)110;     // [UInt8:n, UInt8:Sign, n:nums]
    public static final byte LARGE_BIG_EXT = (byte)111;     // [UInt32:n, UInt8:Sign, n:nums]
    public static final byte NEW_FUN_EXT = (byte)112;       // [UInt32:Size, UInt8:Arity, 16*Uint6-MD5:Uniq, UInt32:Index, UInt32:NumFree, atom:Module, int:OldIndex, int:OldUniq, pid:Pid, NunFree*ext:FreeVars]
    public static final byte EXPORT_EXT = (byte)113;        // [atom:Module, atom:Function, smallint:Arity]
    public static final byte NEW_REFERENCE_EXT = (byte)114; // [UInt16:Len, atom:Node, UInt8:Creation, Len*UInt32:ID]
    public static final byte SMALL_ATOM_EXT = (byte)115;    // [UInt8:Len, Len:AtomName]
    public static final byte FUN_EXT = (byte)117;           // [UInt4:NumFree, pid:Pid, atom:Module, int:Index, int:Uniq, NumFree*ext:FreeVars]
    public static final byte COMPRESSED = (byte)80;         // [UInt4:UncompressedSize, N:ZlibCompressedData]

    public static ByteBuffer term_to_binary(JsonElement elem, int compression_level)
        throws IOException, IllegalArgumentException, UnsupportedEncodingException {

        ByteBuffer ubuf = term_to_binary(elem);
        if (compression_level >= 0) {
            if (compression_level > 9) { compression_level = 6; }
            ByteBuffer cbuf = compress(ubuf.array(), compression_level);
            if (cbuf.limit() < ubuf.limit()) {
                ByteBuffer result = ByteBuffer.allocate(Byte.BYTES + Byte.BYTES + Integer.BYTES + cbuf.limit())
                    .order(ByteOrder.BIG_ENDIAN).put(FORMAT_VERSION).put(COMPRESSED).putInt(ubuf.limit()).put(cbuf.array(), 0, cbuf.limit());
                result.flip();
                return result;
            }
        }
        ByteBuffer result = ByteBuffer.allocate(Byte.BYTES + ubuf.limit())
            .order(ByteOrder.BIG_ENDIAN).put(FORMAT_VERSION).put(ubuf.array(), 0, ubuf.limit());
        result.flip();
        return result;
    }

    public static ByteBuffer term_to_binary(JsonObject obj) throws IllegalArgumentException, UnsupportedEncodingException {
        int count = 0;
        ByteBuffer buf = ByteBuffer.allocate(0);
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            ByteBuffer key = term_to_binary(entry.getKey()), value = term_to_binary(entry.getValue());
            buf = concat(buf, ByteBuffer.allocate(Byte.BYTES + Byte.BYTES + key.limit() + value.limit())
                .order(ByteOrder.BIG_ENDIAN).put(SMALL_TUPLE_EXT).put((byte)2).put(key.array(), 0, key.limit()).put(value.array(), 0, value.limit()));
            ++count;
        }
        ByteBuffer result = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + buf.limit() + Byte.BYTES);
        result.order(ByteOrder.BIG_ENDIAN).put(LIST_EXT).putInt(count);
        if (count > 0) {
            result.put(buf.array(), 0, buf.limit());
        }
        result.put(NIL_EXT);
        return result;
    }
    public static ByteBuffer term_to_binary(JsonArray arr) throws IllegalArgumentException, UnsupportedEncodingException {
        int count = 0;
        ByteBuffer buf = ByteBuffer.allocate(0);
        for (int i = 0; i < arr.size(); i++) {
            buf = concat(buf, term_to_binary(arr.get(i)));
            ++count;
        }
        if (count == 0) {
            return ByteBuffer.allocate(Byte.BYTES).order(ByteOrder.BIG_ENDIAN).put(NIL_EXT);
        }
        return ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + buf.limit() + Byte.BYTES)
            .order(ByteOrder.BIG_ENDIAN).put(LIST_EXT).putInt(count).put(buf.array(), 0, buf.limit()).put(NIL_EXT);
    }
    public static ByteBuffer term_to_binary(JsonElement value) throws IllegalArgumentException, UnsupportedEncodingException {
        if (value.isJsonNull()) {
            return term_to_binary(value.getAsJsonNull());
        } else if (value.isJsonObject()) {
            return term_to_binary(value.getAsJsonObject());
        } else if (value.isJsonArray()) {
            return term_to_binary(value.getAsJsonArray());
        } else if (value.isJsonPrimitive()) {
            return term_to_binary(value.getAsJsonPrimitive());
        }
        throw new IllegalArgumentException("unexpected JsonObject value!");
    }
    public static ByteBuffer term_to_binary(JsonPrimitive value) throws IllegalArgumentException, UnsupportedEncodingException {
        if (value.isBoolean()) {
            return term_to_binary(value.getAsBoolean());
        } else if (value.isString()) {
            return term_to_binary(value.getAsString());
        } else if (value.isNumber()) {
            return term_to_binary(value.getAsNumber());
        }
        throw new IllegalArgumentException("unexpected JsonPrimitive value!");
    }
    public static ByteBuffer term_to_binary(JsonNull value) throws UnsupportedEncodingException {
        String str = "none";
        byte[] utf8 = str.getBytes("UTF-8");
        return ByteBuffer.allocate(Byte.BYTES + Short.BYTES + utf8.length)
            .order(ByteOrder.BIG_ENDIAN).put(ATOM_EXT).putShort((short)utf8.length).put(utf8, 0, utf8.length);
    }
    public static ByteBuffer term_to_binary(boolean value) throws UnsupportedEncodingException {
        String str = value ? "true" : "false";
        byte[] utf8 = str.getBytes("UTF-8");
        return ByteBuffer.allocate(Byte.BYTES + Short.BYTES + utf8.length)
            .order(ByteOrder.BIG_ENDIAN).put(ATOM_EXT).putShort((short)utf8.length).put(utf8, 0, utf8.length);
    }
    public static ByteBuffer term_to_binary(String str) throws UnsupportedEncodingException {
        byte[] utf8 = str.getBytes("UTF-8");
        return ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + utf8.length)
            .order(ByteOrder.BIG_ENDIAN).put(BINARY_EXT).putInt((int)utf8.length).put(utf8, 0, utf8.length);
    }
    public static ByteBuffer term_to_binary(Number value) throws UnsupportedEncodingException {
        if (value.longValue() == value.doubleValue()) {
            long val = value.longValue();
            if (0 <= val && val <= 255) {
                return term_to_binary((byte)val);
            } else if (-2147483648 <= val && val <= 2147483647) {
                return term_to_binary((int)val);
            } else {
                return term_to_binary((byte)val);
            }
        } else {
            return term_to_binary(value.doubleValue());
        }
    }
    public static ByteBuffer term_to_binary(byte val) throws UnsupportedEncodingException {
        return ByteBuffer.allocate(Byte.BYTES + Byte.BYTES).order(ByteOrder.BIG_ENDIAN).put(SMALL_INTEGER_EXT).put(val);
    }
    public static ByteBuffer term_to_binary(int val) throws UnsupportedEncodingException {
        return ByteBuffer.allocate(Byte.BYTES + Integer.BYTES).order(ByteOrder.BIG_ENDIAN).put(INTEGER_EXT).putInt(val);
    }
    public static ByteBuffer term_to_binary(long val) throws UnsupportedEncodingException {
        val = Math.abs(val);
        ByteBuffer big_buf = ByteBuffer.allocate(64);
        int len = 0;
        for (len = 0; val > 0; ++len, val >>= 8) {
            big_buf.put((byte)(val & 0xff));
        }
        ByteBuffer buf = ByteBuffer.allocate(Byte.BYTES +
            (len < 256 ? Byte.BYTES : Integer.BYTES) + Byte.BYTES + len);
        buf.order(ByteOrder.BIG_ENDIAN);
        if (len < 256) {
            buf.put(SMALL_BIG_EXT).put((byte)len);
        } else {
            buf.put(LARGE_BIG_EXT).putInt(len);
        }
        buf.put((byte)((val < 0) ? 1 : 0));
        buf.put(big_buf.array(), 0, len);
        return buf;
    }
    public static ByteBuffer term_to_binary(double value) throws UnsupportedEncodingException {
        String str = String.format("%.20e", value);
        byte[] utf8 = str.getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.allocate(Byte.BYTES + 31).order(ByteOrder.BIG_ENDIAN).put(FLOAT_EXT).put(utf8, 0, utf8.length);
        for (int i = 0; i < 31 - utf8.length; ++i) {
            buf.put((byte)0);
        }
        return buf;
    }

    public static ByteBuffer concat(ByteBuffer buf1, ByteBuffer buf2) {
        ByteBuffer buf = ByteBuffer.allocate(buf1.limit() + buf2.limit()).put(buf1.array()).put(buf2.array());
        buf.rewind(); return buf;
    }

    public static ByteBuffer compress(byte[] data, int compression_level) throws IOException {
        Deflater deflater = new Deflater(compression_level);
        deflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        deflater.finish();
        byte[] buffer = new byte[102400];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();
        ByteBuffer result = ByteBuffer.allocate(output.length);
        result.put(output, 0, output.length);
        result.flip();
        return result;
    }

}
