// Package oxiwire implements OxiDB's custom binary wire protocol.
//
// OxiWire is designed for maximum decode speed on the client side.
// All lengths are fixed-size (4 bytes LE), all numbers are fixed-size (8 bytes LE),
// and strings are length-prefixed with no escaping. This eliminates the variable-length
// integer decoding overhead of MsgPack/protobuf.
//
// Type tags (1 byte):
//
//	0x00 = Null
//	0x01 = False
//	0x02 = True
//	0x03 = Int64  (8 bytes LE follow)
//	0x04 = Uint64 (8 bytes LE follow)
//	0x05 = Float64 (8 bytes LE follow)
//	0x06 = String (4-byte LE length + raw bytes)
//	0x07 = Array  (4-byte LE count + values)
//	0x08 = Map    (4-byte LE count + (string-key, value) pairs)
//
// Response envelope:
//
//	[0xDB] [status: 0=ok, 1=error] [value]
//
// Magic byte 0xDB distinguishes OxiWire from JSON (0x7B) and MsgPack (0x80-0xDF range but never 0xDB for a map).
package oxiwire

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

// Type tags
const (
	TagNull   = 0x00
	TagFalse  = 0x01
	TagTrue   = 0x02
	TagInt64  = 0x03
	TagUint64 = 0x04
	TagFloat  = 0x05
	TagString = 0x06
	TagArray  = 0x07
	TagMap    = 0x08

	Magic = 0xDB // First byte of OxiWire response
)

// Decoder reads OxiWire binary data from a byte buffer with zero-copy string access.
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder creates a decoder over the given buffer.
func NewDecoder(buf []byte) *Decoder {
	return &Decoder{buf: buf}
}

// IsOxiWire checks if a response payload uses the OxiWire format.
func IsOxiWire(data []byte) bool {
	return len(data) > 0 && data[0] == Magic
}

// DecodeResponse decodes an OxiWire response envelope.
// Returns (ok bool, data any, err error).
// For ok=true, data is the decoded value.
// For ok=false, data is the error string.
func DecodeResponse(payload []byte) (bool, any, error) {
	if len(payload) < 2 {
		return false, nil, fmt.Errorf("oxiwire: payload too short")
	}
	if payload[0] != Magic {
		return false, nil, fmt.Errorf("oxiwire: bad magic byte 0x%02x", payload[0])
	}
	d := &Decoder{buf: payload, pos: 2}
	status := payload[1]
	val, err := d.decodeValue()
	if err != nil {
		return false, nil, err
	}
	return status == 0, val, nil
}

// DecodeDocArray is the fast path for decoding a find response.
// It expects the data portion to be an array of maps and decodes directly
// into []map[string]any with minimal allocations.
func DecodeDocArray(payload []byte) ([]map[string]any, error) {
	if len(payload) < 2 || payload[0] != Magic || payload[1] != 0 {
		return nil, fmt.Errorf("oxiwire: not an ok response")
	}
	d := &Decoder{buf: payload, pos: 2}
	if d.pos >= len(d.buf) {
		return nil, fmt.Errorf("oxiwire: unexpected end")
	}
	tag := d.buf[d.pos]
	d.pos++
	if tag != TagArray {
		// Single value, not array — wrap in slice
		d.pos--
		val, err := d.decodeValue()
		if err != nil {
			return nil, err
		}
		if m, ok := val.(map[string]any); ok {
			return []map[string]any{m}, nil
		}
		return nil, fmt.Errorf("oxiwire: expected array, got tag 0x%02x", tag)
	}
	count := d.readU32()
	result := make([]map[string]any, count)
	for i := uint32(0); i < count; i++ {
		m, err := d.decodeMap()
		if err != nil {
			return nil, err
		}
		result[i] = m
	}
	return result, nil
}

func (d *Decoder) decodeValue() (any, error) {
	if d.pos >= len(d.buf) {
		return nil, fmt.Errorf("oxiwire: unexpected end at pos %d", d.pos)
	}
	tag := d.buf[d.pos]
	d.pos++

	switch tag {
	case TagNull:
		return nil, nil
	case TagFalse:
		return false, nil
	case TagTrue:
		return true, nil
	case TagInt64:
		return float64(d.readI64()), nil
	case TagUint64:
		return float64(d.readU64()), nil
	case TagFloat:
		return d.readF64(), nil
	case TagString:
		return d.readString(), nil
	case TagArray:
		count := d.readU32()
		arr := make([]any, count)
		for i := uint32(0); i < count; i++ {
			v, err := d.decodeValue()
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case TagMap:
		return d.decodeMapInner()
	default:
		return nil, fmt.Errorf("oxiwire: unknown tag 0x%02x at pos %d", tag, d.pos-1)
	}
}

func (d *Decoder) decodeMap() (map[string]any, error) {
	if d.pos >= len(d.buf) {
		return nil, fmt.Errorf("oxiwire: unexpected end")
	}
	tag := d.buf[d.pos]
	d.pos++
	if tag != TagMap {
		return nil, fmt.Errorf("oxiwire: expected map, got 0x%02x", tag)
	}
	return d.decodeMapInner()
}

func (d *Decoder) decodeMapInner() (map[string]any, error) {
	count := d.readU32()
	m := make(map[string]any, count)
	for i := uint32(0); i < count; i++ {
		key := d.readString()
		val, err := d.decodeValue()
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

// readU32 reads a 4-byte little-endian uint32.
func (d *Decoder) readU32() uint32 {
	v := binary.LittleEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return v
}

// readI64 reads an 8-byte little-endian int64.
func (d *Decoder) readI64() int64 {
	v := int64(binary.LittleEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return v
}

// readU64 reads an 8-byte little-endian uint64.
func (d *Decoder) readU64() uint64 {
	v := binary.LittleEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return v
}

// readF64 reads an 8-byte little-endian float64.
func (d *Decoder) readF64() float64 {
	bits := binary.LittleEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return math.Float64frombits(bits)
}

// readString reads a length-prefixed string. The returned string shares
// the underlying buffer memory (zero-copy) via unsafe conversion.
func (d *Decoder) readString() string {
	length := int(d.readU32())
	s := unsafeString(d.buf[d.pos : d.pos+length])
	d.pos += length
	return s
}

// unsafeString converts a byte slice to string without copying.
// Safe as long as the buffer is not modified after this call.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// ------------------------------------------------------------------
// Encoder — encode Go values into OxiWire binary format
// ------------------------------------------------------------------

// Encoder writes OxiWire binary data into a byte buffer.
type Encoder struct {
	buf []byte
}

// NewEncoder creates an encoder with initial capacity.
func NewEncoder(capacity int) *Encoder {
	return &Encoder{buf: make([]byte, 0, capacity)}
}

// Bytes returns the encoded bytes.
func (e *Encoder) Bytes() []byte {
	return e.buf
}

// BytesWithMagic returns the encoded bytes prefixed with 0xDB magic byte.
func (e *Encoder) BytesWithMagic() []byte {
	result := make([]byte, 1+len(e.buf))
	result[0] = Magic
	copy(result[1:], e.buf)
	return result
}

// Encode encodes any Go value into OxiWire format.
func (e *Encoder) Encode(v any) {
	switch val := v.(type) {
	case nil:
		e.buf = append(e.buf, TagNull)
	case bool:
		if val {
			e.buf = append(e.buf, TagTrue)
		} else {
			e.buf = append(e.buf, TagFalse)
		}
	case int:
		e.encodeInt64(int64(val))
	case int8:
		e.encodeInt64(int64(val))
	case int16:
		e.encodeInt64(int64(val))
	case int32:
		e.encodeInt64(int64(val))
	case int64:
		e.encodeInt64(val)
	case uint:
		e.encodeUint64(uint64(val))
	case uint8:
		e.encodeUint64(uint64(val))
	case uint16:
		e.encodeUint64(uint64(val))
	case uint32:
		e.encodeUint64(uint64(val))
	case uint64:
		e.encodeUint64(val)
	case float32:
		e.encodeFloat64(float64(val))
	case float64:
		e.encodeFloat64(val)
	case string:
		e.encodeString(val)
	case []byte:
		// Encode as string (binary data)
		e.encodeString(string(val))
	case map[string]any:
		e.encodeMap(val)
	case []any:
		e.encodeArray(val)
	case []map[string]any:
		e.buf = append(e.buf, TagArray)
		e.writeU32(uint32(len(val)))
		for _, item := range val {
			e.encodeMap(item)
		}
	case []string:
		e.buf = append(e.buf, TagArray)
		e.writeU32(uint32(len(val)))
		for _, item := range val {
			e.encodeString(item)
		}
	case []float64:
		e.buf = append(e.buf, TagArray)
		e.writeU32(uint32(len(val)))
		for _, item := range val {
			e.encodeFloat64(item)
		}
	default:
		// Fallback: encode as null
		e.buf = append(e.buf, TagNull)
	}
}

func (e *Encoder) encodeInt64(v int64) {
	if v >= 0 {
		e.buf = append(e.buf, TagUint64)
		e.writeU64(uint64(v))
	} else {
		e.buf = append(e.buf, TagInt64)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		e.buf = append(e.buf, b[:]...)
	}
}

func (e *Encoder) encodeUint64(v uint64) {
	e.buf = append(e.buf, TagUint64)
	e.writeU64(v)
}

func (e *Encoder) encodeFloat64(v float64) {
	e.buf = append(e.buf, TagFloat)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	e.buf = append(e.buf, b[:]...)
}

func (e *Encoder) encodeString(s string) {
	e.buf = append(e.buf, TagString)
	e.writeU32(uint32(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *Encoder) encodeMap(m map[string]any) {
	e.buf = append(e.buf, TagMap)
	e.writeU32(uint32(len(m)))
	for k, v := range m {
		// Map keys: bare string (length + bytes, no type tag)
		e.writeU32(uint32(len(k)))
		e.buf = append(e.buf, k...)
		e.Encode(v)
	}
}

func (e *Encoder) encodeArray(arr []any) {
	e.buf = append(e.buf, TagArray)
	e.writeU32(uint32(len(arr)))
	for _, item := range arr {
		e.Encode(item)
	}
}

func (e *Encoder) writeU32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

func (e *Encoder) writeU64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	e.buf = append(e.buf, b[:]...)
}

// Marshal encodes a value into OxiWire bytes with 0xDB magic prefix.
func Marshal(v any) []byte {
	enc := NewEncoder(256)
	enc.Encode(v)
	return enc.BytesWithMagic()
}
