//+build !art

package roaring64

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/tidwall/btree"
	"math"
	"strconv"
	"sync"
)

func New(values ...uint64) *BTreemap {
	tm := &BTreemap{
		tree: btree.New(2, nil),
	}
	tm.AddMany(values)
	return tm.WithCppSerializer()
}

type BTreemap struct {
	tree *btree.BTree
	serializer serializer
}

func (tm *BTreemap) forEachBitmap(callback func(bm *keyedBitmap) bool) {
	tm.tree.Ascend(func(i btree.Item) bool {
		return callback(i.(*keyedBitmap))
	})
}

func (tm *BTreemap) RunOptimize() {
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		bm.RunOptimize()
		return true
	})
}

func (tm *BTreemap) AddMany(values []uint64) {
	for _, v := range values {
		tm.Add(v)
	}
}

func (tm *BTreemap) CheckedAdd(value uint64) bool {
	key, hi, lo, cleanup := tm.makeKey(value)
	defer cleanup()

	bm, found := tm.get(key)
	if found {
		return bm.CheckedAdd(lo)
	}

	bm = &keyedBitmap{Bitmap: roaring.BitmapOf(lo), HighBits: hi}
	tm.tree.ReplaceOrInsert(bm)
	return true
}

func (tm *BTreemap) AddInt(value int) {
	tm.Add(uint64(value))
}

var keyPool = sync.Pool{New: func() interface{} { return &keyedBitmap{}}}
func makeKey(high uint32) (*keyedBitmap, func()) {
	inst := keyPool.Get().(*keyedBitmap)
	inst.HighBits = high
	return inst, func() {
		inst.Bitmap = nil
		inst.HighBits = 0
		keyPool.Put(inst)
	}
}

func (tm *BTreemap) makeKey(value uint64) (btree.Item, uint32, uint32, func()) {
	hi, lo := splitHiLo(value)
	key, cleanup := makeKey(hi)
	return key, hi, lo, cleanup
}

func (tm *BTreemap) Add(value uint64) {
	key, hi, lo, cleanup := tm.makeKey(value)
	defer cleanup()

	bm, found := tm.get(key)
	if found {
		bm.Add(lo)
		return
	}

	bm = &keyedBitmap{Bitmap: roaring.BitmapOf(lo), HighBits: hi}
	tm.tree.ReplaceOrInsert(bm)
}

func (tm *BTreemap) IsEmpty() bool {
	isEmpty := true
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		if bm.IsEmpty() {
			return true
		}
		isEmpty = false
		return false
	})
	return isEmpty
}

func (tm *BTreemap) Clear() {
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		bm.Clear()
		return true
	})
}

func (tm *BTreemap) Contains(value uint64) bool {
	key, _, lo, cleanup := tm.makeKey(value)
	defer cleanup()

	bm, found := tm.get(key)
	if !found {
		return false
	}
	return bm.Contains(lo)
}

func (tm *BTreemap) CheckedRemove(value uint64) bool {
	key, _, lo, cleanup := tm.makeKey(value)
	defer cleanup()

	bm, found := tm.get(key)
	if !found {
		return false
	}

	removed := bm.CheckedRemove(lo)
	if bm.IsEmpty() {
		deleted := tm.tree.Delete(key)
		return deleted != nil || removed
	}
	return removed
}

func (tm *BTreemap) Remove(value uint64) {
	key, _, lo, cleanup := tm.makeKey(value)
	defer cleanup()

	bm, found := tm.get(key)
	if !found {
		return
	}

	bm.Remove(lo)
	if bm.IsEmpty() {
		tm.tree.Delete(key)
	}
}

func (tm *BTreemap) GetCardinality() uint64 {
	var card uint64
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		if bm.IsEmpty() {
			return true
		}
		card += bm.GetCardinality()
		return true
	})
	return card
}

func (tm *BTreemap) Minimum() uint64 {
	min := tm.tree.Min()
	if min == nil {
		return 0
	}
	bm := min.(*keyedBitmap)
	return joinHiLo(bm.HighBits, bm.Minimum())
}

func (tm *BTreemap) Maximum() uint64 {
	max := tm.tree.Max()
	if max == nil {
		return 0
	}
	bm := max.(*keyedBitmap)
	return joinHiLo(bm.HighBits, bm.Maximum())
}

func (tm *BTreemap) And(other *BTreemap) {
	var toRemove []btree.Item
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		rbm, found := other.get(bm)
		if !found {
			toRemove = append(toRemove, bm)
			return true
		}

		bm.Bitmap.And(rbm.Bitmap)
		if bm.IsEmpty() {
			toRemove = append(toRemove, bm)
		}
		return true
	})
	for _, key := range toRemove {
		tm.tree.Delete(key)
	}
}

func (tm *BTreemap) AndCardinality(other *BTreemap) uint64 {
	l, r := tm, other
	if tm.tree.Len() < other.tree.Len() {
		l, r = r, l
	}

	var total uint64
	l.forEachBitmap(func(bm *keyedBitmap) bool {
		rbm, found := r.get(bm)
		if !found {
			return true
		}
		total += rbm.Bitmap.AndCardinality(bm.Bitmap)
		return true
	})
	return total
}

func (tm *BTreemap) Or(other *BTreemap) {
	other.forEachBitmap(func(bm *keyedBitmap) bool {
		cur, found := tm.get(bm)
		if !found {
			tm.tree.ReplaceOrInsert(bm.ClonePtr())
			return true
		}

		cur.Or(bm.Bitmap)
		return true
	})
}


func (tm *BTreemap) OrCardinality(other *BTreemap) uint64 {
	var total uint64

	seenKey := make(map[uint32]bool)
	other.forEachBitmap(func(cbm *keyedBitmap) bool {
		seenKey[cbm.HighBits] = true
		cur, found := tm.get(cbm)
		if !found {
			total += cbm.GetCardinality()
			return true
		}
		total += cur.OrCardinality(cbm.Bitmap)
		return true
	})

	tm.forEachBitmap(func(cbm *keyedBitmap) bool {
		if seenKey[cbm.HighBits] {
			return true
		}

		total += cbm.GetCardinality()
		return true
	})
	return total
}


func (tm *BTreemap) Xor(other *BTreemap) {
	var toRemove []btree.Item
	other.forEachBitmap(func(bm *keyedBitmap) bool {
		cur, found := tm.get(bm)
		if !found {
			tm.tree.ReplaceOrInsert(bm.ClonePtr())
			return true
		}

		cur.Xor(bm.Bitmap)
		if cur.IsEmpty() {
			toRemove = append(toRemove, bm)
		}
		return true
	})
	for _, key := range toRemove {
		tm.tree.Delete(key)
	}
}

func (tm *BTreemap) AndNot(other *BTreemap) {
	tm.forEachBitmap(func(node *keyedBitmap) bool {
		obm, found := other.get(node)
		if found {
			node.AndNot(obm.Bitmap)
		}
		return true
	})
}

func (tm *BTreemap) get(bm btree.Item) (*keyedBitmap, bool) {
	n := tm.tree.Get(bm)
	if n != nil {
		return n.(*keyedBitmap), true
	}
	return nil, false
}

func (tm *BTreemap) getOrInsert(highBits uint32) *keyedBitmap {
	key, cleanup := makeKey(highBits)
	defer cleanup()

	ebm, gotEnd := tm.get(key)
	if !gotEnd {
		ebm = &keyedBitmap{
			Bitmap:   roaring.New(),
			HighBits: highBits,
		}
		tm.tree.ReplaceOrInsert(ebm)
	}
	return ebm
}

func (tm *BTreemap) ToArray() []uint64 {
	var res []uint64
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		bm.Bitmap.Iterate(func(x uint32) bool {
			res = append(res, joinHiLo(bm.HighBits, x))
			return true
		})
		return true
	})
	return res
}

func (tm *BTreemap) Clone() *BTreemap {
	cloned := New()
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		cloned.tree.ReplaceOrInsert(bm.ClonePtr())
		return true
	})
	return cloned
}

func (tm *BTreemap) ContainsInt(x int) bool {
	return tm.Contains(uint64(x))
}

func (tm *BTreemap) Equals(o interface{}) bool {
	other, cast := o.(*BTreemap)
	if !cast {
		return false
	}

	if other.tree.Len() != tm.tree.Len() {
		return false
	}

	equals := true
	tm.forEachBitmap(func(node *keyedBitmap) bool {
		obm, found := other.get(node)
		if !found {
			equals = false
			return false
		}

		if !node.Equals(obm.Bitmap) {
			equals = false
			return false
		}
		return true
	})
	return equals
}

func (tm *BTreemap) Intersects(other *BTreemap) bool {
	l, r := tm, other
	if other.tree.Len() > tm.tree.Len() {
		l, r = r, l
	}

	var intersects bool
	l.forEachBitmap(func(node *keyedBitmap) bool {
		rbm, found := r.get(node)
		if !found {
			return true
		}

		if node.Intersects(rbm.Bitmap) {
			intersects = true
			return false
		}
		return true
	})
	return intersects
}

func (tm *BTreemap) Rank(value uint64) uint64 {
	var result uint64
	hi, lo := splitHiLo(value)
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		if bm.HighBits > hi {
			return false
		}
		if bm.HighBits < hi {
			result += bm.GetCardinality()
			return true
		}
		result += bm.Rank(lo)
		return false
	})
	return result
}

func (tm *BTreemap) Select(value uint64) (uint64, error) {
	sz := tm.GetCardinality()
	if sz <= value {
		return 0, fmt.Errorf("can't find %dth integer in a bitmap with only %d items", value, sz)
	}
	var result uint64
	var failed bool
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		card := bm.GetCardinality()
		if value >= card {
			value -= card
			return true
		}

		v, err := bm.Select(uint32(value))
		if err != nil {
			failed = true
			return false
		}
		result = joinHiLo(bm.HighBits, v)
		return false
	})
	if failed {
		return 0, fmt.Errorf("can't find %dth integer in a bitmap with only %d items", value, sz)
	}
	return result, nil
}


func (tm *BTreemap) String() string {
	// inspired by https://github.com/fzandona/goroar/
	var buffer bytes.Buffer
	start := []byte("{")
	buffer.Write(start)
	i := tm.Iterator()
	counter := 0
	if i.HasNext() {
		counter = counter + 1
		buffer.WriteString(strconv.FormatInt(int64(i.Next()), 10))
	}
	for i.HasNext() {
		buffer.WriteString(",")
		counter = counter + 1
		// to avoid exhausting the memory
		if counter > 0x40000 {
			buffer.WriteString("...")
			break
		}
		buffer.WriteString(strconv.FormatInt(int64(i.Next()), 10))
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (tm *BTreemap) Stats() (stats roaring.Statistics) {
	tm.forEachBitmap(func(bm *keyedBitmap) bool {
		st := bm.Bitmap.Stats()
		stats.Cardinality += st.Cardinality
		stats.Containers += st.Containers

		stats.ArrayContainers += st.ArrayContainerBytes
		stats.ArrayContainerBytes += st.ArrayContainerBytes
		stats.ArrayContainerValues += st.ArrayContainerValues

		stats.BitmapContainers += st.BitmapContainers
		stats.BitmapContainerBytes += st.BitmapContainerBytes
		stats.BitmapContainerValues += st.BitmapContainerValues

		stats.RunContainers += st.RunContainers
		stats.RunContainerBytes += st.RunContainerBytes
		stats.RunContainerValues += st.RunContainerValues
		return true
	})
	return
}

func (tm *BTreemap) Flip(rangeStart, rangeEnd uint64) {
	hiStart, loStart := splitHiLo(rangeStart)
	hiEnd, loEnd := splitHiLo(rangeEnd)

	if hiStart == hiEnd {
		tm.getOrInsert(hiStart).Flip(uint64(loStart), uint64(loEnd))
		return
	}

	bm := tm.getOrInsert(hiStart)
	bm.Bitmap.Flip(uint64(loStart), math.MaxUint32)

	for cur := hiStart+1; cur < loEnd-1; cur++ {
		cbm := tm.getOrInsert(cur)
		cbm.Flip(0, math.MaxUint32)
	}

	ebm := tm.getOrInsert(hiEnd)
	ebm.Flip(0, uint64(loEnd))
}

func (tm *BTreemap) FlipInt(rangeStart, rangeEnd int) {
	tm.Flip(uint64(rangeStart), uint64(rangeEnd))
}


func (tm *BTreemap) AddRange(rangeStart, rangeEnd uint64) {
	hiStart, loStart := splitHiLo(rangeStart)
	hiEnd, loEnd := splitHiLo(rangeEnd)

	if hiStart == hiEnd {
		tm.getOrInsert(hiStart).AddRange(uint64(loStart), uint64(loEnd))
		return
	}

	bm := tm.getOrInsert(hiStart)
	bm.Bitmap.AddRange(uint64(loStart), math.MaxUint32)

	for cur := hiStart+1; cur < loEnd-1; cur++ {
		cbm := tm.getOrInsert(cur)
		cbm.AddRange(0, math.MaxUint32)
	}

	ebm := tm.getOrInsert(hiEnd)
	ebm.AddRange(0, uint64(loEnd))
}

func (tm *BTreemap) RemoveRange(rangeStart, rangeEnd uint64) {
	hiStart, loStart := splitHiLo(rangeStart)
	hiEnd, loEnd := splitHiLo(rangeEnd)

	if hiStart == hiEnd {
		bm := tm.getOrInsert(hiStart)
		bm.RemoveRange(uint64(loStart), uint64(loEnd))
		if bm.IsEmpty() {
			tm.tree.Delete(bm)
		}
		return
	}

	bm := tm.getOrInsert(hiStart)
	bm.Bitmap.RemoveRange(uint64(loStart), math.MaxUint32)

	for cur := hiStart+1; cur < loEnd-1; cur++ {
		cbm := tm.getOrInsert(cur)
		cbm.RemoveRange(0, math.MaxUint32)
	}

	ebm := tm.getOrInsert(hiEnd)
	ebm.RemoveRange(0, uint64(loEnd))
}
