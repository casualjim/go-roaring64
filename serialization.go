package roaring64

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/tidwall/btree"
	"io"
	"sync"
)

// serializer that is compatible with C++ version found in
// CRoaring at https://github.com/RoaringBitmap/CRoaring/blob/master/cpp/roaring64map.hh
func (tm *BTreemap) WithCppSerializer() *BTreemap {
	tm.serializer = &cppSerializer{tm}
	return tm
}

// serializer that is compatible with JVM version of Treemap
// found in RoaringBitmap Java implementation at:
// https://github.com/RoaringBitmap/RoaringBitmap/blob/master/roaringbitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java
func (tm *BTreemap) WithJvmSerializer() *BTreemap {
	tm.serializer = &jvmSerializer{tm}
	return tm
}

func (tm *BTreemap) ToBase64() (string, error) {
	buf := new(bytes.Buffer)
	_, err := tm.WriteTo(buf)
	return base64.StdEncoding.EncodeToString(buf.Bytes()), err
}

func (tm *BTreemap) FromBase64(str string) (int64, error) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return 0, err
	}
	buf := bytes.NewReader(data)
	return tm.ReadFrom(buf)
}

func (tm *BTreemap) WriteTo(stream io.Writer) (int64, error) {
	return tm.serializer.WriteTo(stream)
}

func (tm *BTreemap) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	_, err := tm.WriteTo(&buf)
	return buf.Bytes(), err
}

func (tm *BTreemap) ReadFrom(reader io.Reader) (p int64, err error) {
	return tm.serializer.ReadFrom(reader)
}

func (tm *BTreemap) FromBuffer(buf []byte) (p int64, err error) {
	pl := getByteBuffer()
	defer putByteBuffer(pl)

	if _, err := pl.Write(buf); err != nil {
		return 0, err
	}
	return tm.ReadFrom(pl)
}

func (tm *BTreemap) MarshalBinary() ([]byte, error) {
	return tm.ToBytes()
}

func (tm *BTreemap) UnmarshalBinary(data []byte) error {
	_, err := tm.ReadFrom(bytes.NewReader(data))
	return err
}

func (tm *BTreemap) GetSizeInBytes() uint64 {
	n := 8 + (uint64(tm.tree.Len())*4)
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		n += bm.GetSizeInBytes()
		return true
	})
	return n
}

func (tm *BTreemap) GetSerializedSizeInBytes() uint64 {
	n := 8 + (uint64(tm.tree.Len())*4)
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		n += bm.GetSerializedSizeInBytes()
		return true
	})
	return n
}

var byteBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

func getByteBuffer() *bytes.Buffer {
	return byteBufferPool.Get().(*bytes.Buffer)
}

func putByteBuffer(buf *bytes.Buffer) {
	buf.Reset()
}

type serializer interface {
	io.WriterTo
	io.ReaderFrom
}

type cppSerializer struct {
	tm *BTreemap
}

func (c *cppSerializer) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.LittleEndian, uint64(c.tm.tree.Len())); err != nil {
		return 0, err
	}

	n := int64(8)
	var err error
	c.tm.forEachBitmap(func(bm *keyedBitmap) bool {
		if err = binary.Write(w, binary.LittleEndian, bm.HighBits); err != nil {
			return false
		}
		n += 4
		nn, er := bm.WriteTo(w)
		if er != nil {
			err = er
			return false
		}
		n += nn
		return true
	})
	return n, err
}

func (c *cppSerializer) ReadFrom(r io.Reader) (n int64, err error) {
	tm := btree.New(2, nil)
	var sz uint64
	if err = binary.Read(r, binary.LittleEndian, &sz); err != nil {
		return
	}

	n = int64(8)
	for i := uint64(0); i < sz; i++ {
		var highBits uint32
		if err = binary.Read(r, binary.LittleEndian, &highBits); err != nil {
			return
		}
		n += 4
		bm := roaring.New()
		if nn, err := bm.ReadFrom(r); err != nil {
			return n, err
		} else {
			n += nn
		}
		tm.ReplaceOrInsert(&keyedBitmap{
			Bitmap:   bm,
			HighBits: highBits,
		})
	}
	c.tm.tree = tm
	return
}

type jvmSerializer struct {
	tm *BTreemap
}

func (j *jvmSerializer) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, false); err != nil {
		return 0, err
	}
	n := int64(1)

	if err := binary.Write(w, binary.BigEndian, uint32(j.tm.tree.Len())); err != nil {
		return n, err
	}
	n += 4

	var err error
	j.tm.forEachBitmap(func(bm *keyedBitmap) bool {
		if err = binary.Write(w, binary.BigEndian, bm.HighBits); err != nil {
			return false
		}
		n += 4
		nn, er := bm.WriteTo(w)
		if er != nil {
			err = er
			return false
		}
		n += nn
		return true
	})
	return n, err
}

func (j *jvmSerializer) ReadFrom(r io.Reader) (n int64, err error) {
	tm := btree.New(2, nil)
	var _bl bool
	if err = binary.Read(r, binary.BigEndian, &_bl); err != nil {
		return
	}

	var sz uint32
	if err = binary.Read(r, binary.BigEndian, &sz); err != nil {
		return
	}

	n = int64(8)
	for i := uint32(0); i < sz; i++ {
		var highBits uint32
		if err = binary.Read(r, binary.BigEndian, &highBits); err != nil {
			return
		}
		n += 4
		bm := roaring.New()
		if nn, err := bm.ReadFrom(r); err != nil {
			return n, err
		} else {
			n += nn
		}
		tm.ReplaceOrInsert(&keyedBitmap{
			Bitmap:   bm,
			HighBits: highBits,
		})
	}
	j.tm.tree = tm
	return
}
