package roaring64

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/tidwall/btree"
	"io"
)

const (
	maxLowBit = 0xffffffff
)

func splitHiLo(val uint64) (uint32, uint32) {
	return uint32(val >> 32), uint32(val)
}

func joinHiLo(hi uint32, lo uint32) uint64 {
	return (uint64(hi) << 32) | uint64(lo)
}

type keyedBitmap struct {
	*roaring.Bitmap
	HighBits uint32
}

func (kb *keyedBitmap) Less(than btree.Item, ctx interface{}) bool {
	return kb.HighBits < than.(*keyedBitmap).HighBits
}


func (kb keyedBitmap) Clone() keyedBitmap {
	return keyedBitmap{
		HighBits: kb.HighBits,
		Bitmap: kb.Bitmap.Clone(),
	}
}


func (kb *keyedBitmap) ClonePtr() *keyedBitmap {
	return &keyedBitmap{
		HighBits: kb.HighBits,
		Bitmap: kb.Bitmap.Clone(),
	}
}

type Bitmap64 interface {
	ToBase64() (string, error)
	FromBase64(str string) (int64, error)
	WriteTo(stream io.Writer) (int64, error)
	ToBytes() ([]byte, error)
	ReadFrom(reader io.Reader) (p int64, err error)
	FromBuffer(buf []byte) (p int64, err error)
	RunOptimize()
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	Clear()
	ToArray() []uint64
	GetSizeInBytes() uint64
	GetSerializedSizeInBytes() uint64
	String() string
	Iterate(cb func(x uint64) bool)
	Iterator() IntPeekable
	ReverseIterator() IntIterable
	ManyIterator() ManyIntIterable
	Clone() *BTreemap
	Minimum() uint64
	Maximum() uint64
	Contains(x uint64) bool
	ContainsInt(x int) bool
	Equals(o interface{}) bool
	Add(x uint64)
	CheckedAdd(x uint64) bool
	AddInt(x int)
	Remove(x uint64)
	CheckedRemove(x uint64) bool
	IsEmpty() bool
	GetCardinality() uint64
	And(other *BTreemap)
	OrCardinality(other *BTreemap) uint64
	AndCardinality(other *BTreemap) uint64
	Intersects(other *BTreemap) bool
	Xor(other *BTreemap)
	Or(other *BTreemap)
	AndNot(other *BTreemap)
	AddMany(dat []uint64)
	Rank(x uint64) uint64
	Select(x uint64) (uint64, error)
	Flip(rangeStart, rangeEnd uint64)
	FlipInt(rangeStart, rangeEnd int)
	AddRange(rangeStart, rangeEnd uint64)
	RemoveRange(rangeStart, rangeEnd uint64)
	Stats() roaring.Statistics
}


// IntIterable allows you to iterate over the values in a Bitmap
type IntIterable interface {
	HasNext() bool
	Next() uint64
}

// IntPeekable allows you to look at the next value without advancing and
// advance as long as the next value is smaller than minval
type IntPeekable interface {
	IntIterable
	// PeekNext peeks the next value without advancing the iterator
	PeekNext() uint64
	// AdvanceIfNeeded advances as long as the next value is smaller than minval
	AdvanceIfNeeded(minval uint64)
}


// ManyIntIterable allows you to iterate over the values in a Bitmap
type ManyIntIterable interface {
	// pass in a buffer to fill up with values, returns how many values were returned
	NextMany([]uint64) int
}
