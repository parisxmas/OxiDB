using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace OxiDb.Client.Tcp;

/// <summary>
/// OxiWire — OxiDB's custom binary wire protocol for maximum decode speed.
///
/// Type tags (1 byte):
///   0x00 = Null
///   0x01 = False
///   0x02 = True
///   0x03 = Int64  (8 bytes LE)
///   0x04 = Uint64 (8 bytes LE)
///   0x05 = Float64 (8 bytes LE)
///   0x06 = String (4-byte LE length + raw bytes)
///   0x07 = Array  (4-byte LE count + values)
///   0x08 = Map    (4-byte LE count + (string-key, value) pairs)
///
/// Response envelope: [0xDB] [status: 0=ok, 1=error] [value]
/// Request envelope:  [0xDB] [value]
/// </summary>
public static class OxiWire
{
    public const byte Magic = 0xDB;

    private const byte TagNull = 0x00;
    private const byte TagFalse = 0x01;
    private const byte TagTrue = 0x02;
    private const byte TagInt64 = 0x03;
    private const byte TagUint64 = 0x04;
    private const byte TagFloat = 0x05;
    private const byte TagString = 0x06;
    private const byte TagArray = 0x07;
    private const byte TagMap = 0x08;

    /// <summary>
    /// Check if a payload is OxiWire format (starts with 0xDB magic byte).
    /// </summary>
    public static bool IsOxiWire(ReadOnlySpan<byte> data) =>
        data.Length > 0 && data[0] == Magic;

    // ── Encoder ─────────────────────────────────────────────────────

    /// <summary>
    /// Encode a request payload (Dictionary) as OxiWire with magic prefix.
    /// </summary>
    public static byte[] EncodeRequest(Dictionary<string, object?> payload)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        buf.GetSpan(1)[0] = Magic;
        buf.Advance(1);
        EncodeValue(payload, buf);
        return buf.WrittenSpan.ToArray();
    }

    private static void EncodeValue(object? value, IBufferWriter<byte> buf)
    {
        switch (value)
        {
            case null:
                WriteByte(buf, TagNull);
                break;

            case bool b:
                WriteByte(buf, b ? TagTrue : TagFalse);
                break;

            case int i:
                if (i >= 0)
                {
                    WriteByte(buf, TagUint64);
                    WriteUInt64(buf, (ulong)i);
                }
                else
                {
                    WriteByte(buf, TagInt64);
                    WriteInt64(buf, i);
                }
                break;

            case long l:
                if (l >= 0)
                {
                    WriteByte(buf, TagUint64);
                    WriteUInt64(buf, (ulong)l);
                }
                else
                {
                    WriteByte(buf, TagInt64);
                    WriteInt64(buf, l);
                }
                break;

            case uint u:
                WriteByte(buf, TagUint64);
                WriteUInt64(buf, u);
                break;

            case ulong ul:
                WriteByte(buf, TagUint64);
                WriteUInt64(buf, ul);
                break;

            case double d:
                WriteByte(buf, TagFloat);
                WriteFloat64(buf, d);
                break;

            case float f:
                WriteByte(buf, TagFloat);
                WriteFloat64(buf, f);
                break;

            case string s:
                EncodeString(s, buf);
                break;

            case byte[] bytes:
                // Encode raw bytes as base64 string (for blob data)
                EncodeString(Convert.ToBase64String(bytes), buf);
                break;

            case Dictionary<string, object?> map:
                WriteByte(buf, TagMap);
                WriteUInt32(buf, (uint)map.Count);
                foreach (var (key, val) in map)
                {
                    // Map keys are bare strings (length + bytes, no type tag)
                    WriteUInt32(buf, (uint)Encoding.UTF8.GetByteCount(key));
                    WriteStringBytes(key, buf);
                    EncodeValue(val, buf);
                }
                break;

            case string[] strArr:
                WriteByte(buf, TagArray);
                WriteUInt32(buf, (uint)strArr.Length);
                foreach (var s in strArr)
                    EncodeString(s, buf);
                break;

            case object[] arr:
                WriteByte(buf, TagArray);
                WriteUInt32(buf, (uint)arr.Length);
                foreach (var item in arr)
                    EncodeValue(item, buf);
                break;

            case IEnumerable<object> enumerable:
                var list = enumerable.ToList();
                WriteByte(buf, TagArray);
                WriteUInt32(buf, (uint)list.Count);
                foreach (var item in list)
                    EncodeValue(item, buf);
                break;

            default:
                // Fallback: serialize to JSON string, encode that
                var json = JsonSerializer.Serialize(value);
                EncodeString(json, buf);
                break;
        }
    }

    private static void EncodeString(string s, IBufferWriter<byte> buf)
    {
        WriteByte(buf, TagString);
        var byteCount = Encoding.UTF8.GetByteCount(s);
        WriteUInt32(buf, (uint)byteCount);
        WriteStringBytes(s, buf);
    }

    private static void WriteByte(IBufferWriter<byte> buf, byte value)
    {
        buf.GetSpan(1)[0] = value;
        buf.Advance(1);
    }

    private static void WriteUInt32(IBufferWriter<byte> buf, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buf.GetSpan(4), value);
        buf.Advance(4);
    }

    private static void WriteUInt64(IBufferWriter<byte> buf, ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(buf.GetSpan(8), value);
        buf.Advance(8);
    }

    private static void WriteInt64(IBufferWriter<byte> buf, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(buf.GetSpan(8), value);
        buf.Advance(8);
    }

    private static void WriteFloat64(IBufferWriter<byte> buf, double value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(buf.GetSpan(8), BitConverter.DoubleToUInt64Bits(value));
        buf.Advance(8);
    }

    private static void WriteStringBytes(string s, IBufferWriter<byte> buf)
    {
        var byteCount = Encoding.UTF8.GetByteCount(s);
        var span = buf.GetSpan(byteCount);
        Encoding.UTF8.GetBytes(s, span);
        buf.Advance(byteCount);
    }

    // ── Decoder ─────────────────────────────────────────────────────

    /// <summary>
    /// Decode an OxiWire response envelope.
    /// Returns (ok, data) where ok indicates success/error.
    /// </summary>
    public static (bool Ok, JsonElement Data) DecodeResponse(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 2 || payload[0] != Magic)
            throw new OxiDbTcpException("OxiWire: invalid magic byte");

        var status = payload[1];
        var pos = 2;

        // Decode the value by writing to a Utf8JsonWriter, then parse as JsonDocument
        var buffer = new ArrayBufferWriter<byte>(payload.Length * 2);
        using var writer = new Utf8JsonWriter(buffer);
        DecodeValueToJson(payload, ref pos, writer);
        writer.Flush();

        using var doc = JsonDocument.Parse(buffer.WrittenMemory);
        var element = doc.RootElement.Clone();

        return (status == 0, element);
    }

    /// <summary>
    /// Decode an OxiWire value and write it as JSON to a Utf8JsonWriter.
    /// This avoids building intermediate objects — goes straight from binary to JSON.
    /// </summary>
    private static void DecodeValueToJson(ReadOnlySpan<byte> buf, ref int pos, Utf8JsonWriter writer)
    {
        if (pos >= buf.Length)
            throw new OxiDbTcpException("OxiWire: unexpected end of input");

        var tag = buf[pos++];

        switch (tag)
        {
            case TagNull:
                writer.WriteNullValue();
                break;

            case TagFalse:
                writer.WriteBooleanValue(false);
                break;

            case TagTrue:
                writer.WriteBooleanValue(true);
                break;

            case TagInt64:
                EnsureBytes(buf, pos, 8);
                writer.WriteNumberValue(BinaryPrimitives.ReadInt64LittleEndian(buf[pos..]));
                pos += 8;
                break;

            case TagUint64:
                EnsureBytes(buf, pos, 8);
                writer.WriteNumberValue(BinaryPrimitives.ReadUInt64LittleEndian(buf[pos..]));
                pos += 8;
                break;

            case TagFloat:
                EnsureBytes(buf, pos, 8);
                var bits = BinaryPrimitives.ReadUInt64LittleEndian(buf[pos..]);
                writer.WriteNumberValue(BitConverter.UInt64BitsToDouble(bits));
                pos += 8;
                break;

            case TagString:
                EnsureBytes(buf, pos, 4);
                var strLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);
                pos += 4;
                EnsureBytes(buf, pos, strLen);
                writer.WriteStringValue(buf.Slice(pos, strLen));
                pos += strLen;
                break;

            case TagArray:
                EnsureBytes(buf, pos, 4);
                var arrCount = BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);
                pos += 4;
                writer.WriteStartArray();
                for (uint i = 0; i < arrCount; i++)
                    DecodeValueToJson(buf, ref pos, writer);
                writer.WriteEndArray();
                break;

            case TagMap:
                EnsureBytes(buf, pos, 4);
                var mapCount = BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);
                pos += 4;
                writer.WriteStartObject();
                for (uint i = 0; i < mapCount; i++)
                {
                    // Map keys: bare string (length + bytes, no type tag)
                    EnsureBytes(buf, pos, 4);
                    var keyLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(buf[pos..]);
                    pos += 4;
                    EnsureBytes(buf, pos, keyLen);
                    writer.WritePropertyName(buf.Slice(pos, keyLen));
                    pos += keyLen;
                    DecodeValueToJson(buf, ref pos, writer);
                }
                writer.WriteEndObject();
                break;

            default:
                throw new OxiDbTcpException($"OxiWire: unknown tag 0x{tag:X2} at position {pos - 1}");
        }
    }

    private static void EnsureBytes(ReadOnlySpan<byte> buf, int pos, int need)
    {
        if (pos + need > buf.Length)
            throw new OxiDbTcpException($"OxiWire: truncated data at position {pos}, need {need} bytes");
    }
}
