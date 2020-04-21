package roaring64

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/tidwall/btree"
)

func (tm *BTreemap) Iterate(cb func(x uint64) bool) {
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		var goOn bool
		bm.Bitmap.Iterate(func(x uint32) bool {
			goOn = cb(joinHiLo(bm.HighBits, x))
			return goOn
		})
		return goOn
	})
}

func (tm *BTreemap) Iterator() IntPeekable {
	iter := &u64Iterator{
		hiIter: tm.tree.Cursor(),
	}
	nxt := iter.hiIter.First()
	if nxt != nil {
		iter.next = nxt.(*keyedBitmap)
		iter.loIter = iter.next.Iterator()
	}

	return iter
}

func (tm *BTreemap) ReverseIterator() IntIterable {
	iter := &u64ReverseIterator{
		hiIter: tm.tree.Cursor(),
	}
	nxt := iter.hiIter.First()
	if nxt != nil {
		iter.next = nxt.(*keyedBitmap)
		iter.loIter = iter.next.Iterator()
	}

	return iter
}

func (tm *BTreemap) ManyIterator() ManyIntIterable {
	iter := &u64ManyIterator{
		hiIter: tm.tree.Cursor(),
	}
	nxt := iter.hiIter.First()
	if nxt != nil {
		iter.next = nxt.(*keyedBitmap)
		iter.loIter = iter.next.ManyIterator()
	}

	return iter
}

type u64Iterator struct {
	next      *keyedBitmap
	hiIter    *btree.Cursor
	loIter    roaring.IntPeekable
}

func (u *u64Iterator) PeekNext() uint64 {
	peekable, ok := u.loIter.(roaring.IntPeekable)
	if !ok {
		return 0
	}
	return joinHiLo(u.next.HighBits, peekable.PeekNext())
}

func (u *u64Iterator) AdvanceIfNeeded(minval uint64) {
	hi, lo := splitHiLo(minval)
	key, cleanup := makeKey(hi)
	defer cleanup()

	if u.next.HighBits > hi {
		return
	}

	candidate := u.hiIter.Seek(key)
	if candidate == nil || candidate.(*keyedBitmap).HighBits != hi {
		return
	}
	bm := candidate.(*keyedBitmap)
	iter := bm.Iterator()
	iter.AdvanceIfNeeded(lo)

	u.next = bm
	u.loIter = iter
}

func (u *u64Iterator) HasNext() bool {
	return u.next != nil && u.loIter.HasNext()
}

func (u *u64Iterator) Next() uint64 {
	result := joinHiLo(u.next.HighBits, u.loIter.Next())
	if !u.loIter.HasNext() {
		nx := u.hiIter.Next()
		if nx == nil {
			u.next = nil
			return result
		}
		u.next = nx.(*keyedBitmap)
		u.loIter = u.next.Iterator()
	}

	return result
}

type u64ReverseIterator struct {
	next   *keyedBitmap
	hiIter *btree.Cursor
	loIter roaring.IntIterable
}

func (u *u64ReverseIterator) HasNext() bool {
	return u.next != nil && u.loIter.HasNext()
}

func (u *u64ReverseIterator) Next() uint64 {
	result := joinHiLo(u.next.HighBits, u.loIter.Next())

	if !u.loIter.HasNext() {
		nx := u.hiIter.Prev()
		if nx == nil {
			u.next = nil
			return result
		}
		u.next = nx.(*keyedBitmap)

		u.loIter = u.next.ReverseIterator()
	}

	return result
}


type u64ManyIterator struct {
	next   *keyedBitmap
	hiIter *btree.Cursor
	loIter roaring.ManyIntIterable
}

func (u u64ManyIterator) NextMany(uint64s []uint64) (n int) {
	ln := len(uint64s)
	uint32s := make([]uint32, len(uint64s))
	for n < ln {
		if u.next == nil {
			break
		}

		nn := u.loIter.NextMany(uint32s[n:])
		n += nn
		if nn == 0 {
			nx := u.hiIter.Next()
			if nx == nil {
				break
			}
			u.next = nx.(*keyedBitmap)
			u.loIter = u.next.ManyIterator()
		}
	}
	return
}

