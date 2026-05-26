package com.oxidb.client;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Encoder/decoder for the OxiWire binary protocol.
 *
 * Type tags:
 *   0x00 = Null
 *   0x01 = False
 *   0x02 = True
 *   0x03 = Int64  (8 bytes LE)
 *   0x04 = Uint64 (8 bytes LE)
 *   0x05 = Float64 (8 bytes LE)
 *   0x06 = String (4 byte LE length + UTF-8 bytes)
 *   0x07 = Array  (4 byte LE count + values)
 *   0x08 = Map    (4 byte LE count + (bare-string key, value) pairs)
 *
 * Magic byte: 0xDB
 * Request:  [0xDB][encoded map]
 * Response: [0xDB][status: 0=ok, 1=error][encoded value]
 *
 * Framing: [4 byte LE length][payload]
 */
public final class OxiWireCodec {

    public static final byte MAGIC = (byte) 0xDB;

    static final byte TAG_NULL   = 0x00;
    static final byte TAG_FALSE  = 0x01;
    static final byte TAG_TRUE   = 0x02;
    static final byte TAG_INT64  = 0x03;
    static final byte TAG_UINT64 = 0x04;
    static final byte TAG_FLOAT  = 0x05;
    static final byte TAG_STRING = 0x06;
    static final byte TAG_ARRAY  = 0x07;
    static final byte TAG_MAP    = 0x08;

    private OxiWireCodec() {}

    // ── Encoding ──────────────────────────────────────────────

    public static byte[] encodeRequest(Map<String, Object> command) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(256);
        buf.write(MAGIC & 0xFF);
        encodeValue(buf, command);
        return buf.toByteArray();
    }

    @SuppressWarnings("unchecked")
    static void encodeValue(ByteArrayOutputStream buf, Object value) {
        if (value == null) {
            buf.write(TAG_NULL);
        } else if (value instanceof Boolean b) {
            buf.write(b ? TAG_TRUE : TAG_FALSE);
        } else if (value instanceof Long l) {
            buf.write(TAG_INT64);
            writeLE8(buf, l);
        } else if (value instanceof Integer i) {
            buf.write(TAG_INT64);
            writeLE8(buf, i.longValue());
        } else if (value instanceof Double d) {
            buf.write(TAG_FLOAT);
            writeLE8(buf, Double.doubleToRawLongBits(d));
        } else if (value instanceof Float f) {
            buf.write(TAG_FLOAT);
            writeLE8(buf, Double.doubleToRawLongBits(f.doubleValue()));
        } else if (value instanceof Number n) {
            buf.write(TAG_FLOAT);
            writeLE8(buf, Double.doubleToRawLongBits(n.doubleValue()));
        } else if (value instanceof String s) {
            buf.write(TAG_STRING);
            writeBareString(buf, s);
        } else if (value instanceof List<?> list) {
            buf.write(TAG_ARRAY);
            writeLE4(buf, list.size());
            for (Object item : list) {
                encodeValue(buf, item);
            }
        } else if (value instanceof Map<?, ?> map) {
            buf.write(TAG_MAP);
            writeLE4(buf, map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeBareString(buf, entry.getKey().toString());
                encodeValue(buf, entry.getValue());
            }
        } else {
            buf.write(TAG_STRING);
            writeBareString(buf, value.toString());
        }
    }

    private static void writeBareString(ByteArrayOutputStream buf, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeLE4(buf, bytes.length);
        buf.write(bytes, 0, bytes.length);
    }

    private static void writeLE4(ByteArrayOutputStream buf, int value) {
        buf.write(value & 0xFF);
        buf.write((value >> 8) & 0xFF);
        buf.write((value >> 16) & 0xFF);
        buf.write((value >> 24) & 0xFF);
    }

    private static void writeLE8(ByteArrayOutputStream buf, long value) {
        for (int i = 0; i < 8; i++) {
            buf.write((int) ((value >> (i * 8)) & 0xFF));
        }
    }

    // ── Framing ───────────────────────────────────────────────

    public static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        byte[] lenBuf = new byte[4];
        ByteBuffer.wrap(lenBuf).order(ByteOrder.LITTLE_ENDIAN).putInt(payload.length);
        out.write(lenBuf);
        out.write(payload);
        out.flush();
    }

    public static byte[] readFrame(InputStream in) throws IOException {
        byte[] lenBuf = readExact(in, 4);
        int len = ByteBuffer.wrap(lenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (len < 0 || len > 16 * 1024 * 1024) {
            throw new IOException("Invalid frame length: " + len);
        }
        return readExact(in, len);
    }

    private static byte[] readExact(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int read = in.read(buf, off, n - off);
            if (read < 0) throw new IOException("Unexpected end of stream");
            off += read;
        }
        return buf;
    }

    // ── Decoding ──────────────────────────────────────────────

    public static OxiWireResponse decodeResponse(byte[] payload) throws IOException {
        if (payload.length < 2 || (payload[0] & 0xFF) != (MAGIC & 0xFF)) {
            throw new IOException("Invalid OxiWire response: missing magic byte");
        }
        int status = payload[1] & 0xFF;
        int[] pos = {2};
        Object value = decodeValue(payload, pos);
        return new OxiWireResponse(status == 0, value);
    }

    static Object decodeValue(byte[] data, int[] pos) throws IOException {
        if (pos[0] >= data.length) {
            throw new IOException("Unexpected end of data at position " + pos[0]);
        }
        int tag = data[pos[0]++] & 0xFF;
        return switch (tag) {
            case TAG_NULL -> null;
            case TAG_FALSE -> Boolean.FALSE;
            case TAG_TRUE -> Boolean.TRUE;
            case TAG_INT64 -> readLELong(data, pos);
            case TAG_UINT64 -> readLELong(data, pos); // treat as long in Java
            case TAG_FLOAT -> Double.longBitsToDouble(readLELong(data, pos));
            case TAG_STRING -> readString(data, pos);
            case TAG_ARRAY -> {
                int count = readLEInt(data, pos);
                List<Object> list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    list.add(decodeValue(data, pos));
                }
                yield list;
            }
            case TAG_MAP -> {
                int count = readLEInt(data, pos);
                LinkedHashMap<String, Object> map = new LinkedHashMap<>(count);
                for (int i = 0; i < count; i++) {
                    String key = readBareString(data, pos);
                    Object val = decodeValue(data, pos);
                    map.put(key, val);
                }
                yield map;
            }
            default -> throw new IOException("Unknown type tag: 0x" + Integer.toHexString(tag));
        };
    }

    private static long readLELong(byte[] data, int[] pos) throws IOException {
        if (pos[0] + 8 > data.length) throw new IOException("Truncated int64");
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= ((long) (data[pos[0] + i] & 0xFF)) << (i * 8);
        }
        pos[0] += 8;
        return v;
    }

    private static int readLEInt(byte[] data, int[] pos) throws IOException {
        if (pos[0] + 4 > data.length) throw new IOException("Truncated int32");
        int v = (data[pos[0]] & 0xFF)
              | ((data[pos[0] + 1] & 0xFF) << 8)
              | ((data[pos[0] + 2] & 0xFF) << 16)
              | ((data[pos[0] + 3] & 0xFF) << 24);
        pos[0] += 4;
        return v;
    }

    private static String readString(byte[] data, int[] pos) throws IOException {
        int len = readLEInt(data, pos);
        if (pos[0] + len > data.length) throw new IOException("Truncated string");
        String s = new String(data, pos[0], len, StandardCharsets.UTF_8);
        pos[0] += len;
        return s;
    }

    // Bare string: no type tag, just [4B len][bytes]
    private static String readBareString(byte[] data, int[] pos) throws IOException {
        return readString(data, pos); // same encoding, the tag was already consumed at the map level
    }

    // ── Response wrapper ──────────────────────────────────────

    public record OxiWireResponse(boolean ok, Object value) {
        public String errorMessage() {
            if (ok) return null;
            return value != null ? value.toString() : "Unknown error";
        }
    }
}
