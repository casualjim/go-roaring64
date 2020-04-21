package roaring64

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"testing"
)

func u64(in uint32) uint64  {
	return joinHiLo(in, in)
}

func TestTreemap_Add(t *testing.T) {
	bm := New()
	n1 := u64(3)
	bm.Add(n1)
	require.True(t, bm.Contains(n1))

	bm.Add(math.MaxUint32)
	require.True(t, bm.Contains(math.MaxUint32))

	u64 := uint64(math.MaxUint32) + 1
	bm.Add(u64)
	require.True(t, bm.Contains(u64))
}

func TestTreemap_IsEmpty(t *testing.T) {
	bm := New()
	require.True(t, bm.IsEmpty())
	bm.Add(math.MaxUint64)
	require.False(t, bm.IsEmpty())
}

func TestTreemap_Clear(t *testing.T) {
	bm := New()
	bm.Add(1)
	bm.Add(math.MaxUint64)
	require.False(t, bm.IsEmpty())
	bm.Clear()
	require.True(t, bm.IsEmpty())
}

func TestTreemap_Remove(t *testing.T) {
	bm := New()
	bm.Add(math.MaxUint64)
	require.False(t, bm.IsEmpty())
	bm.Remove(math.MaxUint64)
	require.True(t, bm.IsEmpty())
}

func TestTreemap_GetCardinality(t *testing.T) {
	bm := New()
	require.EqualValues(t, 0, bm.GetCardinality())
	bm.Add(1)
	require.EqualValues(t, 1, bm.GetCardinality())
	bm.Add(math.MaxUint64)
	require.EqualValues(t, 2, bm.GetCardinality())
}

func TestTreemap_Minimum(t *testing.T) {
	bm := New()
	n1 := u64(120)
	bm.Add(n1)
	bm.Add(u64(1000))
	require.EqualValues(t, n1, bm.Minimum())
}

func TestTreemap_Maximum(t *testing.T) {
	bm := New()
	n1 := u64(1000)
	bm.Add(u64(64))
	bm.Add(n1)
	require.EqualValues(t, n1, bm.Maximum())
}

func TestTreemap_And(t *testing.T) {
	bm1 := New(math.MaxUint64)
	bm2 := New(25)
	bm3 := New(math.MaxUint64)
	bm4 := New(math.MaxUint64, 25)

	bm1.And(bm2)
	require.EqualValues(t, 0, bm1.GetCardinality())
	require.False(t, bm1.Contains(math.MaxUint64))
	require.False(t, bm1.Contains(25))

	bm3.And(bm4)
	require.EqualValues(t, 1, bm3.GetCardinality())
	require.True(t, bm3.Contains(math.MaxUint64))
	require.False(t, bm3.Contains(25))

	bm5 := New(math.MaxUint64)
	bm5.And(New())
	require.EqualValues(t, 0, bm5.GetCardinality())
}

func TestTreemap_Or(t *testing.T) {
	bm1 := New(15)
	bm2 := New(25)
	bm3 := New(15)
	bm4 := New(15, 25)

	bm1.Or(bm2)
	require.EqualValues(t, 2, bm1.GetCardinality())
	require.True(t, bm1.Contains(15))
	require.True(t, bm1.Contains(25))
	require.False(t, bm2.Contains(15))

	bm3.Or(bm4)
	require.EqualValues(t, 2, bm3.GetCardinality())
	require.True(t, bm3.Contains(15))
	require.True(t, bm3.Contains(25))
	require.True(t, bm4.Contains(15))
}

func TestTreemap_ToArray(t *testing.T) {
	bm := New(100, 20, 1, 15, math.MaxUint64, 4, 10)
	exp := []uint64{1, 4, 10, 15, 20, 100, math.MaxUint64}
	require.Equal(t, exp, bm.ToArray())
}

func TestTreemap_Equals(t *testing.T) {
	bm1 := New(10, 20, math.MaxUint32, math.MaxUint64)
	bm2 := New(math.MaxUint32, math.MaxUint64, 10, 20)
	bm3 := New(10, 20)
	bm4 := New()

	require.True(t, bm1.Equals(bm2))
	require.False(t, bm1.Equals(bm3))
	require.True(t, bm3.Equals(bm3))
	require.False(t, bm1.Equals(bm4))
	require.True(t, bm4.Equals(bm4))
}

func TestTreemap_Intersects(t *testing.T) {
	bm1 := New(8, 18, 32)
	bm2 := New()
	bm3 := New(6, 93, 23)
	bm4 := New(8, 353, 32334)

	require.False(t, bm1.Intersects(bm2))
	require.False(t, bm1.Intersects(bm3))
	require.True(t, bm1.Intersects(bm1))
	require.True(t, bm1.Intersects(bm4))
}

func TestTreemap_AndCardinality(t *testing.T) {
	bm := New()
	bm.Add(1)
	for i := uint64(21); i <= 260_000; i++ {
		bm.Add(i)
	}

	bm2 := New(25)

	require.EqualValues(t, 1, bm2.AndCardinality(bm))
	require.Equal(t, bm.GetCardinality(), bm2.OrCardinality(bm))
	require.EqualValues(t, 1, bm.AndCardinality(bm2))
	require.Equal(t, bm.GetCardinality(), bm.OrCardinality(bm2))
	require.EqualValues(t, 1, bm2.AndCardinality(bm))
	require.Equal(t, bm.GetCardinality(), bm2.OrCardinality(bm))

	bm.RunOptimize()

	require.EqualValues(t, 1, bm2.AndCardinality(bm))
	require.Equal(t, bm.GetCardinality(), bm2.OrCardinality(bm))
	require.EqualValues(t, 1, bm.AndCardinality(bm2))
	require.Equal(t, bm.GetCardinality(), bm.OrCardinality(bm2))
	require.EqualValues(t, 1, bm2.AndCardinality(bm))
	require.Equal(t, bm.GetCardinality(), bm2.OrCardinality(bm))
}

func TestTreemap_Xor(t *testing.T) {
	bm1 := New(15, 25)
	bm2 := New(25, 35)

	bm1.Xor(bm2)

	require.EqualValues(t, 2, bm1.GetCardinality())
	require.True(t, bm1.Contains(15))
	require.True(t, bm1.Contains(35))

	bm3 := New(15)
	bm3.Xor(New())
	require.EqualValues(t, 1, bm3.GetCardinality())
	require.True(t, bm3.Contains(15))
}

func TestTreemap_AndNot(t *testing.T) {
	bm1 := New(15, 25, math.MaxUint64-10)
	bm2 := New(25, 35)

	bm1.AndNot(bm2)

	require.EqualValues(t, 2, bm1.GetCardinality())
	require.True(t, bm1.Contains(15))
	require.True(t, bm1.Contains(math.MaxUint64-10))
	require.False(t, bm1.Contains(math.MaxUint64))
	require.False(t, bm1.Contains(35))

	bm3 := New(15)
	bm3.AndNot(New())
	require.EqualValues(t, 1, bm3.GetCardinality())
	require.True(t, bm3.Contains(15))
}

func TestTreemap_RankQuick(t *testing.T) {
	r := New()
	for i := uint32(1); i < 8194; i += 2 {
		r.Add(u64(i))
	}

	rank := r.Rank(u64(63))
	require.EqualValues(t, 32, rank)
}

func TestTreemap_Rank(t *testing.T) {
	for N := uint64(1); N <= 16384; N *= 2 {
		t.Run("rank tests"+strconv.Itoa(int(N)), func(t *testing.T) {
			for gap := uint64(1); gap <= 65536; gap *= 2 {
				rb1 := New()
				var known []uint64
				for x := uint32(1); x <= uint32(N); x += uint32(gap) {
					v := u64(x)
					known = append(known, v)
					rb1.Add(v)
				}
				assert.EqualValues(t, 0, rb1.Rank(0))
				for y := uint64(0); y < uint64(len(known)); y++ {
					yy := known[int(y)]
					if rb1.Rank(yy) != y+1 {
						assert.Equal(t, y+1, rb1.Rank(yy))
					}
				}
			}
		})
	}
}


func TestBitmap_Select(t *testing.T) {
	for N := uint64(1); N <= 8192; N *= 2 {
		t.Run("rank tests"+strconv.Itoa(int(N)), func(t *testing.T) {
			for gap := uint64(1); gap <= 65536; gap *= 2 {
				rb1 := New()
				for x := uint32(0); x <= uint32(N); x += uint32(gap) {
					rb1.Add(u64(x))
				}
				for y := uint64(0); y <= N/gap; y++ {
					expectedInt := u64(uint32(y * gap))
					i, err := rb1.Select(y)
					if err != nil {
						t.Fatal(err)
					}

					if i != expectedInt {
						assert.Equal(t, expectedInt, i)
					}
				}
			}
		})
	}
}

// taken from https://github.com/RoaringBitmap/gocroaring/blob/11e8ef92e0473dc8dc09bf28c2a6c9b5240e72b8/gocroaring_test.go#L103
func TestSmoke(t *testing.T) {
	rb1 := New()
	rb1.Add(1)
	rb1.Add(2)
	rb1.Add(3)
	rb1.Add(4)
	rb1.Add(5)
	rb1.Add(100)
	rb1.Add(1000)
	rb1.RunOptimize()
	rb2 := New()
	rb2.Add(3)
	rb2.Add(4)
	rb2.Add(1000)
	rb2.RunOptimize()
	rb3 := New()
	fmt.Println("Cardinality: ", rb1.GetCardinality())
	if rb1.GetCardinality() != 7 {
		t.Error("Bad card")
	}
	if !rb1.Contains(3) {
		t.Error("should contain it")
	}
	rb1.And(rb2)
	fmt.Println(rb1)
	rb3.Add(5)
	rb3.Or(rb1)
	// prints 3, 4, 5, 1000
	i := rb3.Iterator()
	for i.HasNext() {
		fmt.Println(i.Next())
	}
	fmt.Println()
	fmt.Println(rb3.ToArray())
	fmt.Println(rb3)
	//rb4 := FastOr(rb1, rb2, rb3)
	//fmt.Println(rb4)
	// next we include an example of serialization
	//buf := make([]byte, rb1.GetSerializedSizeInBytes())
	//rb1.Write(buf) // we omit error handling
	//newrb, _ := Read(buf)
	//if rb1.Equals(newrb) {
	//	fmt.Println("I wrote the content to a byte stream and read it back.")
	//} else {
	//	t.Error("Bad read")
	//}
}
