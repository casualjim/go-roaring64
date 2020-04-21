//+build art

package roaring64

import (
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	art "github.com/plar/go-adaptive-radix-tree"
)

func New(values ...uint64) *Treemap {
	tm := &Treemap{
		tree: art.New(),
	}
	tm.AddMany(values)
	return tm
}

type Treemap struct {
	tree art.Tree
}

func (tm *Treemap) forEachBitmap(callback func(bm keyedBitmap) bool) {
	tm.tree.ForEach(func(n art.Node) bool {
		return callback(n.Value().(keyedBitmap))
	})
}

func (tm *Treemap) RunOptimize() {
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		bm.RunOptimize()
		return true
	})
}

func (tm *Treemap) AddMany(values []uint64) {
	for _, v := range values {
		tm.Add(v)
	}
}

func (tm *Treemap) CheckedAdd(value uint64) bool {
	key, hi, lo := tm.makeKey(value)

	val, found := tm.tree.Search(key)
	var bm keyedBitmap
	if found {
		bm = val.(keyedBitmap)
		return bm.CheckedAdd(lo)
	}

	bm = keyedBitmap{Bitmap: roaring.BitmapOf(lo), HighBits: hi}
	tm.tree.Insert(key, bm)
	return true
}

func (tm *Treemap) AddInt(value int) {
	tm.Add(uint64(value))
}
func (tm *Treemap) Add(value uint64) {
	key, hi, lo := tm.makeKey(value)

	val, found := tm.tree.Search(key)
	var bm keyedBitmap
	if found {
		bm = val.(keyedBitmap)
		bm.Add(lo)
		return
	}
	bm = keyedBitmap{Bitmap: roaring.BitmapOf(lo), HighBits: hi}
	tm.tree.Insert(key, bm)
}

func (tm *Treemap) IsEmpty() bool {
	isEmpty := true
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		if bm.IsEmpty() {
			return true
		}
		isEmpty = false
		return false
	})
	return isEmpty
}

func (tm *Treemap) Clear() {
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		bm.Clear()
		return true
	})
}

func (tm *Treemap) Contains(value uint64) bool {
	key, _, lo := tm.makeKey(value)
	val, found := tm.tree.Search(key)
	if !found {
		return false
	}

	bm := val.(keyedBitmap)
	return bm.Contains(lo)
}

func (tm *Treemap) CheckedRemove(value uint64) bool {
	key, _, lo := tm.makeKey(value)

	val, found := tm.tree.Search(key)
	var bm keyedBitmap
	if !found {
		return false
	}
	bm = val.(keyedBitmap)
	removed := bm.CheckedRemove(lo)
	if bm.IsEmpty() {
		_, deleted := tm.tree.Delete(key)
		return deleted || removed
	}
	return removed
}

func (tm *Treemap) Remove(value uint64) {
	key, _, lo := tm.makeKey(value)

	val, found := tm.tree.Search(key)
	var bm keyedBitmap
	if !found {
		return
	}
	bm = val.(keyedBitmap)
	bm.Remove(lo)
	if bm.IsEmpty() {
		tm.tree.Delete(key)
	}
}

func (tm *Treemap) GetCardinality() uint64 {
	var card uint64
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		if bm.IsEmpty() {
			return true
		}
		card += bm.GetCardinality()
		return true
	})
	return card
}

func (tm *Treemap) Minimum() uint64 {
	min, hasMin := tm.tree.Minimum()
	if !hasMin {
		return 0
	}
	bm := min.(keyedBitmap)
	return joinHiLo(bm.HighBits, bm.Minimum())
}

func (tm *Treemap) Maximum() uint64 {
	max, hasMin := tm.tree.Maximum()
	if !hasMin {
		return 0
	}
	bm := max.(keyedBitmap)
	return joinHiLo(bm.HighBits, bm.Maximum())
}

func (tm *Treemap) And(other *Treemap) {
	var toRemove []art.Key
	tm.tree.ForEach(func(bm art.Node) bool {
		rbm, found := other.get(bm.Key())
		if !found {
			toRemove = append(toRemove, bm.Key())
			return true
		}

		lbm := bm.Value().(keyedBitmap)
		lbm.Bitmap.And(rbm.Bitmap)
		if lbm.IsEmpty() {
			toRemove = append(toRemove, bm.Key())
		}
		return true
	})
	for _, key := range toRemove {
		tm.tree.Delete(key)
	}
}

func (tm *Treemap) AndCardinality(other *Treemap) uint64 {
	l, r := tm, other
	if tm.tree.Size() < other.tree.Size() {
		l, r = r, l
	}

	var total uint64
	l.tree.ForEach(func(n art.Node) bool {
		rbm, found := r.get(n.Key())
		if !found {
			return true
		}
		lbm := n.Value().(keyedBitmap)
		total += rbm.Bitmap.AndCardinality(lbm.Bitmap)
		return true
	})
	return total
}

func (tm *Treemap) Or(other *Treemap) {
	other.tree.ForEach(func(node art.Node) bool {
		cur, found := tm.get(node.Key())
		if !found {
			tm.tree.Insert(cloneNode(node))
			return true
		}
		cbm := node.Value().(keyedBitmap)
		cur.Or(cbm.Bitmap)
		return true
	})
}


func (tm *Treemap) OrCardinality(other *Treemap) uint64 {
	var total uint64

	seenKey := make(map[uint32]bool)
	other.tree.ForEach(func(node art.Node) bool {
		cbm := node.Value().(keyedBitmap)
		seenKey[cbm.HighBits] = true
		cur, found := tm.get(node.Key())
		if !found {
			total += cbm.GetCardinality()
			return true
		}
		total += cur.OrCardinality(cbm.Bitmap)
		return true
	})

	tm.forEachBitmap(func(cbm keyedBitmap) bool {
		if seenKey[cbm.HighBits] {
			return true
		}

		total += cbm.GetCardinality()
		return true
	})
	return total
}


func (tm *Treemap) Xor(other *Treemap) {
	var toRemove []art.Key
	other.tree.ForEach(func(node art.Node) bool {
		cur, found := tm.get(node.Key())
		if !found {
			tm.tree.Insert(cloneNode(node))
			return true
		}
		cbm := node.Value().(keyedBitmap)
		cur.Xor(cbm.Bitmap)
		if cur.IsEmpty() {
			toRemove = append(toRemove, node.Key())
		}
		return true
	})
	for _, key := range toRemove {
		tm.tree.Delete(key)
	}
}

func (tm *Treemap) AndNot(other *Treemap) {
	tm.tree.ForEach(func(node art.Node) bool {
		obm, found := other.get(node.Key())
		if found {
			cbm := node.Value().(keyedBitmap)
			cbm.AndNot(obm.Bitmap)
		}
		return true
	})
}

func (tm *Treemap) get(key art.Key) (keyedBitmap, bool) {
	n, found := tm.tree.Search(key)
	if found {
		return n.(keyedBitmap), true
	}
	return keyedBitmap{}, false
}

func cloneNode(node art.Node) (art.Key, art.Value) {
	bm := node.Value().(keyedBitmap)
	return append(art.Key{}, node.Key()...), bm.Clone()
}

func (tm *Treemap) makeKey(value uint64) ([]byte, uint32, uint32) {
	hi, lo := splitHiLo(value)

	key := make(art.Key, 4)
	binary.BigEndian.PutUint32(key, hi)
	return key, hi, lo
}

func (tm *Treemap) ToArray() []uint64 {
	var res []uint64
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		bm.Bitmap.Iterate(func(x uint32) bool {
			res = append(res, joinHiLo(bm.HighBits, x))
			return true
		})
		return true
	})
	return res
}

func (tm *Treemap) Clone() *Treemap {
	cloned := New()
	tm.forEachBitmap(func(bm keyedBitmap) bool {
		ck := make([]byte, 4)
		binary.BigEndian.PutUint32(ck, bm.HighBits)
		cloned.tree.Insert(ck, bm.Bitmap.Clone())
		return true
	})
	return cloned
}

func (tm *Treemap) ContainsInt(x int) bool {
	return tm.Contains(uint64(x))
}

func (tm *Treemap) Equals(o interface{}) bool {
	other, cast := o.(*Treemap)
	if !cast {
		return false
	}

	if other.tree.Size() != tm.tree.Size() {
		return false
	}

	equals := true
	tm.tree.ForEach(func(node art.Node) bool {
		obm, found := other.get(node.Key())
		if !found {
			equals = false
			return false
		}

		tbm := node.Value().(keyedBitmap)
		if !tbm.Bitmap.Equals(obm.Bitmap) {
			equals = false
			return false
		}
		return true
	})
	return equals
}

func (tm *Treemap) Intersects(other *Treemap) bool {
	l, r := tm, other
	if other.tree.Size() > tm.tree.Size() {
		l, r = r, l
	}

	var intersects bool
	l.tree.ForEach(func(node art.Node) bool {
		rbm, found := r.get(node.Key())
		if !found {
			return true
		}
		lbm := node.Value().(keyedBitmap)

		if lbm.Intersects(rbm.Bitmap) {
			intersects = true
			return false
		}
		return true
	})
	return intersects
}

func (tm *Treemap) Rank(value uint64) uint64 {
	var result uint64
	hi, lo := splitHiLo(value)
	tm.forEachBitmap(func(bm keyedBitmap) bool {
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

func (tm *Treemap) Select(value uint64) (uint64, error) {
	sz := tm.GetCardinality()
	if sz <= value {
		return 0, fmt.Errorf("can't find %dth integer in a bitmap with only %d items", value, sz)
	}
	var result uint64
	var failed bool
	tm.forEachBitmap(func(bm keyedBitmap) bool {
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


func (tm *Treemap) String() string {
	panic("implement me")
}

func (tm *Treemap) AddRange(rangeStart, rangeEnd uint64) {
	panic("implement me")
}

func (tm *Treemap) RemoveRange(rangeStart, rangeEnd uint64) {
	panic("implement me")
}

func (tm *Treemap) Stats() roaring.Statistics {

	panic("implement me")
}

func (tm *Treemap) Flip(rangeStart, rangeEnd uint64) {
	panic("implement me")
}

func (tm *Treemap) FlipInt(rangeStart, rangeEnd int) {
	panic("implement me")
}
