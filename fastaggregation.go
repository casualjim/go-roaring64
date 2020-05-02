package roaring64

import "container/heap"

// FastAnd computes the intersection between many bitmaps quickly
// Compared to the And function, it can take many bitmaps as input, thus saving the trouble
// of manually calling "And" many times.
func FastAnd(bitmaps ...*BTreemap) *BTreemap {
	if len(bitmaps) == 0 {
		return New()
	} else if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}
	answer := New()
	for _, bm := range bitmaps {
		answer.And(bm)
	}
	return answer
}

// FastOr computes the union between many bitmaps quickly, as opposed to having to call Or repeatedly.
// It might also be faster than calling Or repeatedly.
func FastOr(bitmaps ...*BTreemap) *BTreemap {
	if len(bitmaps) == 0 {
		return New()
	} else if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}

	answer := New()
	for _, bm := range bitmaps {
		answer.Or(bm)
	}
	return answer
}

// FastXor computes the symmetric difference between many bitmaps quickly, as opposed to having to call Or repeatedly.
// It might also be faster than calling Xor repeatedly.
func FastXor(bitmaps ...*BTreemap) *BTreemap {
	if len(bitmaps) == 0 {
		return New()
	} else if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}

	answer := New()
	for _, bm := range bitmaps {
		answer.Xor(bm)
	}
	return answer
}

// HeapOr computes the union between many bitmaps quickly using a heap.
// It might be faster than calling Or repeatedly.
func HeapOr(bitmaps ...*BTreemap) *BTreemap {
	if len(bitmaps) == 0 {
		return New()
	}
	// TODO:  for better speed, we could do the operation lazily, see Java implementation
	pq := make(priorityQueue, len(bitmaps))
	for i, bm := range bitmaps {
		pq[i] = &item{bm, i}
	}
	heap.Init(&pq)

	for pq.Len() > 1 {
		x1 := heap.Pop(&pq).(*item)
		x2 := heap.Pop(&pq).(*item)
		heap.Push(&pq, &item{FastOr(x1.value, x2.value), 0})
	}
	return heap.Pop(&pq).(*item).value
}

// HeapXor computes the symmetric difference between many bitmaps quickly (as opposed to calling Xor repeated).
// Internally, this function uses a heap.
// It might be faster than calling Xor repeatedly.
func HeapXor(bitmaps ...*BTreemap) *BTreemap {
	if len(bitmaps) == 0 {
		return New()
	}

	pq := make(priorityQueue, len(bitmaps))
	for i, bm := range bitmaps {
		pq[i] = &item{bm, i}
	}
	heap.Init(&pq)

	for pq.Len() > 1 {
		x1 := heap.Pop(&pq).(*item)
		x2 := heap.Pop(&pq).(*item)
		heap.Push(&pq, &item{FastXor(x1.value, x2.value), 0})
	}
	return heap.Pop(&pq).(*item).value
}

/////////////
// The priorityQueue is used to keep BTreemaps sorted.
////////////

type item struct {
	value *BTreemap
	index int
}

type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].value.GetSizeInBytes() < pq[j].value.GetSizeInBytes()
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *priorityQueue) update(item *item, value *BTreemap) {
	item.value = value
	heap.Fix(pq, item.index)
}
