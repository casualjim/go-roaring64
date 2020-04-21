package roaring64

import (
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

func TestTreemap_CppSerialize(t *testing.T) {
	f, err := os.Open("_data/testcpp.bin")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	tm := New().WithCppSerializer()
	_, err = tm.ReadFrom(f)
	require.NoError(t, err)

	for i := uint64(100); i < 1000; i++ {
		require.True(t, tm.Contains(i))
	}

	require.True(t, tm.Contains(math.MaxUint32))
	require.True(t, tm.Contains(math.MaxUint64))
}

func TestTreemap_JvmSerialize(t *testing.T) {
	f, err := os.Open("_data/testjvm.bin")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	tm := New().WithJvmSerializer()
	_, err = tm.ReadFrom(f)
	require.NoError(t, err)

	for i := uint64(100); i < 1000; i++ {
		require.True(t, tm.Contains(i))
	}

	require.True(t, tm.Contains(math.MaxUint32))
	require.True(t, tm.Contains(math.MaxUint64))
}
