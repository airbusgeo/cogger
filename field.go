package cogger

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func arrayFieldSize32(data interface{}, bigtiff bool) int {
	ll := 0
	switch d := data.(type) {
	case []uint32:
		ll = len(d)
	case []uint64:
		ll = len(d)
	default:
		panic("bug")
	}
	if bigtiff {
		if ll <= 2 {
			return 20
		}
		return 20 + 4*ll
	} else {
		if ll <= 1 {
			return 12
		}
		return 12 + 4*ll
	}
}

func arrayFieldSize(data interface{}, bigtiff bool) int {
	if bigtiff {
		switch d := data.(type) {
		case []byte:
			if len(d) <= 8 {
				return 20
			}
			return 20 + len(d)
		case []uint16:
			if len(d) <= 4 {
				return 20
			}
			return 20 + 2*len(d)
		case []uint32:
			if len(d) <= 2 {
				return 20
			}
			return 20 + 4*len(d)
		case []uint64:
			if len(d) <= 1 {
				return 20
			}
			return 20 + 8*len(d)
		case []int8:
			if len(d) <= 8 {
				return 20
			}
			return 20 + len(d)
		case []int16:
			if len(d) <= 4 {
				return 20
			}
			return 20 + len(d)*2
		case []int32:
			if len(d) <= 2 {
				return 20
			}
			return 20 + len(d)*4
		case []int64:
			if len(d) <= 1 {
				return 20
			}
			return 20 + len(d)*8
		case []float32:
			if len(d) <= 2 {
				return 20
			}
			return 20 + len(d)*4
		case []float64:
			if len(d) <= 1 {
				return 20
			}
			return 20 + len(d)*8
		case string:
			if len(d) <= 7 {
				return 20
			}
			return 20 + len(d) + 1
		default:
			panic("wrong type")
		}
	} else {
		switch d := data.(type) {
		case []byte:
			if len(d) <= 4 {
				return 12
			}
			return 12 + len(d)
		case []uint16:
			if len(d) <= 2 {
				return 12
			}
			return 12 + 2*len(d)
		case []uint32:
			if len(d) <= 1 {
				return 12
			}
			return 12 + 4*len(d)
		case []int8:
			if len(d) <= 4 {
				return 12
			}
			return 12 + len(d)
		case []int16:
			if len(d) <= 2 {
				return 12
			}
			return 12 + len(d)*2
		case []int32:
			if len(d) <= 1 {
				return 12
			}
			return 12 + len(d)*4
		case []float32:
			if len(d) <= 1 {
				return 12
			}
			return 12 + len(d)*4
		case string:
			if len(d) <= 3 {
				return 12
			}
			return 12 + len(d) + 1
		case []float64:
			return 12 + len(d)*8
		case []int64:
			return 12 + len(d)*8
		case []uint64:
			return 12 + len(d)*8
		default:
			panic("wrong type")
		}
	}
}

func (cog *cog) writeArray32(w io.Writer, tag uint16, data interface{}, tags *tagData) error {
	switch d := data.(type) {
	case []uint64:
		d32 := make([]uint32, len(d))
		for i := range d {
			d32[i] = uint32(d[i])
		}
		return cog.writeArray(w, tag, d32, tags)
	default:
		panic("bug")
	}
}

func (cog *cog) writeArray(w io.Writer, tag uint16, data interface{}, tags *tagData) error {
	var buf []byte
	if cog.bigtiff {
		buf = make([]byte, 20)
	} else {
		buf = make([]byte, 12)
	}
	cog.enc.PutUint16(buf[0:2], tag)
	switch d := data.(type) {
	case []byte:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tByte)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 8 {
				for i := 0; i < n; i++ {
					buf[12+i] = d[i]
				}
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				tags.Write(d)
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			if n <= 4 {
				for i := 0; i < n; i++ {
					buf[8+i] = d[i]
				}
			} else {
				cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
				tags.Write(d)
			}
		}
	case []uint16:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tShort)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 4 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint16(buf[12+i*2:], d[i])
				}
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, d[i]); err != nil {
						return err
					}
				}
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			if n <= 2 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint16(buf[8+i*2:], d[i])
				}
			} else {
				cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, d[i]); err != nil {
						return err
					}
				}
			}
		}
	case []uint32:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tLong)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 2 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint32(buf[12+i*4:], d[i])
				}
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, d[i]); err != nil {
						return err
					}
				}
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			if n <= 1 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint32(buf[8:], d[i])
				}
			} else {
				cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, d[i]); err != nil {
						return err
					}
				}
			}
		}
	case []uint64:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tLong8)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 1 {
				cog.enc.PutUint64(buf[12:], d[0])
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, d[i]); err != nil {
						return err
					}
				}
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
			for i := 0; i < n; i++ {
				if err := binary.Write(tags, cog.enc, d[i]); err != nil {
					return err
				}
			}
		}
	case []float32:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tFloat)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 2 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint32(buf[12+i*4:], math.Float32bits(d[i]))
				}
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, math.Float32bits(d[i])); err != nil {
						return err
					}
				}
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			if n <= 1 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint32(buf[8:], math.Float32bits(d[i]))
				}
			} else {
				cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, math.Float32bits(d[i])); err != nil {
						return err
					}
				}
			}
		}
	case []float64:
		n := len(d)
		cog.enc.PutUint16(buf[2:4], tDouble)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n == 1 {
				for i := 0; i < n; i++ {
					cog.enc.PutUint64(buf[12+i*4:], math.Float64bits(d[0]))
				}
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				for i := 0; i < n; i++ {
					if err := binary.Write(tags, cog.enc, math.Float64bits(d[i])); err != nil {
						return err
					}
				}
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
			for i := 0; i < n; i++ {
				if err := binary.Write(tags, cog.enc, math.Float64bits(d[i])); err != nil {
					return err
				}
			}
		}
	case string:
		n := len(d) + 1
		cog.enc.PutUint16(buf[2:4], tAscii)
		if cog.bigtiff {
			cog.enc.PutUint64(buf[4:12], uint64(n))
			if n <= 8 {
				for i := 0; i < n-1; i++ {
					buf[12+i] = byte(d[i])
				}
				buf[12+n-1] = 0
			} else {
				cog.enc.PutUint64(buf[12:], uint64(tags.NextOffset()))
				tags.Write(append([]byte(d), 0))
			}
		} else {
			cog.enc.PutUint32(buf[4:8], uint32(n))
			if n <= 4 {
				for i := 0; i < n-1; i++ {
					buf[8+i] = d[i]
				}
				buf[8+n-1] = 0
			} else {
				cog.enc.PutUint32(buf[8:], uint32(tags.NextOffset()))
				tags.Write(append([]byte(d), 0))
			}
		}
	default:
		return fmt.Errorf("unsupported type %v", d)
	}
	var err error
	if cog.bigtiff {
		_, err = w.Write(buf[0:20])
	} else {
		_, err = w.Write(buf[0:12])
	}
	return err
}

func (cog *cog) writeField(w io.Writer, tag uint16, data interface{}) error {
	if cog.bigtiff {
		var buf [20]byte
		switch d := data.(type) {
		case byte:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tByte)
			cog.enc.PutUint64(buf[4:12], 1)
			buf[12] = d
		case uint16:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tShort)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint16(buf[12:], d)
		case uint32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tLong)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint32(buf[12:], d)
		case uint64:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tLong8)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint64(buf[12:], d)
		case float32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tFloat)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint32(buf[12:], math.Float32bits(d))
		case float64:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tDouble)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint64(buf[12:], math.Float64bits(d))
		case int8:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSByte)
			cog.enc.PutUint64(buf[4:12], 1)
			buf[12] = byte(d)
		case int16:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSShort)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint16(buf[12:], uint16(d))
		case int32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSLong)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint32(buf[12:], uint32(d))
		case int64:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSLong8)
			cog.enc.PutUint64(buf[4:12], 1)
			cog.enc.PutUint64(buf[12:], uint64(d))
		default:
			panic("unsupported type")
		}
		_, err := w.Write(buf[0:20])
		return err
	} else {
		var buf [12]byte
		switch d := data.(type) {
		case byte:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tByte)
			cog.enc.PutUint32(buf[4:8], 1)
			buf[8] = d
		case uint16:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tShort)
			cog.enc.PutUint32(buf[4:8], 1)
			cog.enc.PutUint16(buf[8:], d)
		case uint32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tLong)
			cog.enc.PutUint32(buf[4:8], 1)
			cog.enc.PutUint32(buf[8:], d)
		case float32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tFloat)
			cog.enc.PutUint32(buf[4:8], 1)
			cog.enc.PutUint32(buf[8:], math.Float32bits(d))
		case int8:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSByte)
			cog.enc.PutUint32(buf[4:8], 1)
			buf[8] = byte(d)
		case int16:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSShort)
			cog.enc.PutUint32(buf[4:8], 1)
			cog.enc.PutUint16(buf[8:], uint16(d))
		case int32:
			cog.enc.PutUint16(buf[0:2], tag)
			cog.enc.PutUint16(buf[2:4], tSLong)
			cog.enc.PutUint32(buf[4:8], 1)
			cog.enc.PutUint32(buf[8:], uint32(d))
		default:
			panic("unsupported type")
		}
		_, err := w.Write(buf[0:12])
		return err
	}
}
