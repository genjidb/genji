package database_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine"
	"github.com/genjidb/genji/engine/memoryengine"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/stretchr/testify/require"
)

// values is a helper function to avoid having to type []document.Value{} all the time.
func values(vs ...document.Value) []document.Value {
	return vs
}

func getIndex(t testing.TB, unique bool, types ...document.ValueType) (*database.Index, func()) {
	ng := memoryengine.NewEngine()
	tx, err := ng.Begin(context.Background(), engine.TxOptions{
		Writable: true,
	})
	require.NoError(t, err)

	idx := database.NewIndex(tx, "foo", &database.IndexInfo{Unique: unique, Types: types})

	return idx, func() {
		tx.Rollback()
	}
}

func TestIndexSet(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Set nil key falls (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.Error(t, idx.Set(values(document.NewBoolValue(true)), nil))
		})

		t.Run(text+"Set value and key succeeds (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()
			require.NoError(t, idx.Set(values(document.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set two values and key succeeds (arity=2)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, document.AnyType, document.AnyType)
			defer cleanup()
			require.NoError(t, idx.Set(values(document.NewBoolValue(true), document.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set one value fails (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, document.AnyType, document.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(document.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set two values fails (arity=1)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, document.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(document.NewBoolValue(true), document.NewBoolValue(true)), []byte("key")))
		})

		t.Run(text+"Set three values fails (arity=2)", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique, document.AnyType, document.AnyType)
			defer cleanup()
			require.Error(t, idx.Set(values(document.NewBoolValue(true), document.NewBoolValue(true), document.NewBoolValue(true)), []byte("key")))
		})
	}

	t.Run("Unique: true, Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, true)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(document.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: integer Duplicate", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, document.IntegerValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(document.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: (integer, integer) Duplicate,", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, document.IntegerValue, document.IntegerValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10), document.NewIntegerValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(10), document.NewIntegerValue(11)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11), document.NewIntegerValue(11)), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(document.NewIntegerValue(10), document.NewIntegerValue(10)), []byte("key")))
	})

	t.Run("Unique: true, Type: (integer, text) Duplicate,", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, document.IntegerValue, document.TextValue)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10), document.NewTextValue("foo")), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11), document.NewTextValue("foo")), []byte("key")))
		require.Equal(t, database.ErrIndexDuplicateValue, idx.Set(values(document.NewIntegerValue(10), document.NewTextValue("foo")), []byte("key")))
	})
}

func TestIndexDelete(t *testing.T) {
	t.Run("Unique: false, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, false)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewDoubleValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(10)), []byte("other-key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11)), []byte("yet-another-key")))
		require.NoError(t, idx.Set(values(document.NewTextValue("hello")), []byte("yet-another-different-key")))
		require.NoError(t, idx.Delete(values(document.NewDoubleValue(10)), []byte("key")))

		pivot := values(document.NewIntegerValue(10))
		i := 0
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			if i == 0 {
				requireEqualBinary(t, testutil.MakeArrayValue(t, 10), v)
				require.Equal(t, "other-key", string(k))
			} else if i == 1 {
				requireEqualBinary(t, testutil.MakeArrayValue(t, 11), v)
				require.Equal(t, "yet-another-key", string(k))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Unique: false, Delete valid key succeeds (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, false, document.AnyType, document.AnyType)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewDoubleValue(10), document.NewDoubleValue(10)), []byte("key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(10), document.NewIntegerValue(10)), []byte("other-key")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(11), document.NewIntegerValue(11)), []byte("yet-another-key")))
		require.NoError(t, idx.Set(values(document.NewTextValue("hello"), document.NewTextValue("hello")), []byte("yet-another-different-key")))
		require.NoError(t, idx.Delete(values(document.NewDoubleValue(10), document.NewDoubleValue(10)), []byte("key")))

		pivot := values(document.NewIntegerValue(10), document.NewIntegerValue(10))
		i := 0
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			if i == 0 {
				expected := document.NewArrayValue(document.NewValueBuffer(
					document.NewIntegerValue(10),
					document.NewIntegerValue(10),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "other-key", string(k))
			} else if i == 1 {
				expected := document.NewArrayValue(document.NewValueBuffer(
					document.NewIntegerValue(11),
					document.NewIntegerValue(11),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "yet-another-key", string(k))
			} else {
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Unique: true, Delete valid key succeeds", func(t *testing.T) {
		idx, cleanup := getIndex(t, true)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10)), []byte("key1")))
		require.NoError(t, idx.Set(values(document.NewDoubleValue(11)), []byte("key2")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(12)), []byte("key3")))
		require.NoError(t, idx.Delete(values(document.NewDoubleValue(11)), []byte("key2")))

		i := 0
		err := idx.AscendGreaterOrEqual(values(document.NewEmptyValue(document.IntegerValue)), func(v, k []byte) error {
			switch i {
			case 0:
				requireEqualBinary(t, testutil.MakeArrayValue(t, 10), v)
				require.Equal(t, "key1", string(k))
			case 1:
				requireEqualBinary(t, testutil.MakeArrayValue(t, 12), v)
				require.Equal(t, "key3", string(k))
			default:
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	t.Run("Unique: true, Delete valid key succeeds (arity=2)", func(t *testing.T) {
		idx, cleanup := getIndex(t, true, document.AnyType, document.AnyType)
		defer cleanup()

		require.NoError(t, idx.Set(values(document.NewIntegerValue(10), document.NewIntegerValue(10)), []byte("key1")))
		require.NoError(t, idx.Set(values(document.NewDoubleValue(11), document.NewDoubleValue(11)), []byte("key2")))
		require.NoError(t, idx.Set(values(document.NewIntegerValue(12), document.NewIntegerValue(12)), []byte("key3")))
		require.NoError(t, idx.Delete(values(document.NewDoubleValue(11), document.NewDoubleValue(11)), []byte("key2")))

		i := 0
		// this will break until the [v, int] case is supported
		// pivot := values(document.NewIntegerValue(0), document.NewEmptyValue(document.IntegerValue))
		pivot := values(document.NewIntegerValue(0), document.NewIntegerValue(0))
		err := idx.AscendGreaterOrEqual(pivot, func(v, k []byte) error {
			switch i {
			case 0:
				expected := document.NewArrayValue(document.NewValueBuffer(
					document.NewIntegerValue(10),
					document.NewIntegerValue(10),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "key1", string(k))
			case 1:
				expected := document.NewArrayValue(document.NewValueBuffer(
					document.NewIntegerValue(12),
					document.NewIntegerValue(12),
				))
				requireEqualBinary(t, expected, v)
				require.Equal(t, "key3", string(k))
			default:
				return errors.New("should not reach this point")
			}

			i++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, i)
	})

	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Delete non existing key fails", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			require.Error(t, idx.Delete(values(document.NewTextValue("foo")), []byte("foo")))
		})
	}
}

func TestIndexExists(t *testing.T) {
	idx, cleanup := getIndex(t, false, document.DoubleValue, document.IntegerValue)
	defer cleanup()

	require.NoError(t, idx.Set(values(document.NewDoubleValue(10), document.NewIntegerValue(11)), []byte("key1")))
	require.NoError(t, idx.Set(values(document.NewDoubleValue(10), document.NewIntegerValue(12)), []byte("key2")))

	ok, key, err := idx.Exists(values(document.NewDoubleValue(10), document.NewIntegerValue(11)))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("key1"), key)

	ok, _, err = idx.Exists(values(document.NewDoubleValue(11), document.NewIntegerValue(11)))
	require.NoError(t, err)
	require.False(t, ok)
}

// requireEqualBinary asserts equality assuming that the value is encoded through marshal binary
func requireEqualBinary(t *testing.T, expected document.Value, actual []byte) {
	t.Helper()

	buf, err := expected.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, buf[:len(buf)-1], actual)
}

type encValue struct {
	skipType bool
	document.Value
}

func requireIdxEncodedEq(t *testing.T, evs ...encValue) func([]byte) {
	t.Helper()

	var buf bytes.Buffer
	for i, ev := range evs {
		if !ev.skipType {
			err := buf.WriteByte(byte(ev.Value.Type()))
			require.NoError(t, err)
		}

		b, err := ev.Value.MarshalBinary()
		require.NoError(t, err)

		_, err = buf.Write(b)
		require.NoError(t, err)

		if i < len(evs)-1 {
			err = buf.WriteByte(document.ArrayValueDelim)
		}
		require.NoError(t, err)
	}

	return func(actual []byte) {
		require.Equal(t, buf.Bytes(), actual)
	}
}

func TestIndexAscendGreaterThan(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			i := 0
			err := idx.AscendGreaterOrEqual(values(document.NewEmptyValue(document.IntegerValue)), func(val, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"Should iterate through documents in order, ", func(t *testing.T) {
			noiseBlob := func(i int) []document.Value {
				t.Helper()
				return []document.Value{document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
			}
			noiseInts := func(i int) []document.Value {
				t.Helper()
				return []document.Value{document.NewIntegerValue(int64(i))}
			}

			noCallEq := func(t *testing.T, i uint8, key []byte, val []byte) {
				require.Fail(t, "equality test should not be called here")
			}

			// the following tests will use that constant to determine how many values needs to be inserted
			// with the value and noise generators.
			total := 5

			tests := []struct {
				name string
				// the index type(s) that is being used
				indexTypes []document.ValueType
				// the pivot, typed or not used to iterate
				pivot database.Pivot
				// the generator for the values that are being indexed
				val func(i int) []document.Value
				// the generator for the noise values that are being indexed
				noise func(i int) []document.Value
				// the function to compare the key/value that the iteration yields
				expectedEq func(t *testing.T, i uint8, key []byte, val []byte)
				// the total count of iteration that should happen
				expectedCount int
				mustPanic     bool
			}{
				// integers ---------------------------------------------------
				{name: "index=any, vals=integers, pivot=integer",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=integer, vals=integers, pivot=integer",
					indexTypes: []document.ValueType{document.IntegerValue},
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=integers, pivot=integer:2",
					indexTypes: nil,
					pivot:      values(document.NewIntegerValue(2)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=integers, pivot=integer:10",
					indexTypes:    nil,
					pivot:         values(document.NewIntegerValue(10)),
					val:           func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=integer, vals=integers, pivot=integer:2",
					indexTypes: []document.ValueType{document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=integer, vals=integers, pivot=double",
					indexTypes:    []document.ValueType{document.IntegerValue},
					pivot:         values(document.NewEmptyValue(document.DoubleValue)),
					val:           func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// doubles ----------------------------------------------------
				{name: "index=any, vals=doubles, pivot=double",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.DoubleValue)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=doubles, pivot=double:1.8",
					indexTypes: nil,
					pivot:      values(document.NewDoubleValue(1.8)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=double, vals=doubles, pivot=double:1.8",
					indexTypes: []document.ValueType{document.DoubleValue},
					pivot:      values(document.NewDoubleValue(1.8)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=doubles, pivot=double:10.8",
					indexTypes:    nil,
					pivot:         values(document.NewDoubleValue(10.8)),
					val:           func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// text -------------------------------------------------------
				{name: "index=any, vals=text pivot=text",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.TextValue)),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('2')",
					indexTypes: nil,
					pivot:      values(document.NewTextValue("2")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=text, pivot=text('')",
					indexTypes: nil,
					pivot:      values(document.NewTextValue("")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('foo')",
					indexTypes:    nil,
					pivot:         values(document.NewTextValue("foo")),
					val:           func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:         noiseInts,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=text, vals=text, pivot=text('2')",
					indexTypes: []document.ValueType{document.TextValue},
					pivot:      values(document.NewTextValue("2")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 3,
				},
				// composite --------------------------------------------------
				// composite indexes can have empty pivot values to iterate on the whole indexed data
				{name: "index=[any, untyped], vals=[int, int], pivot=[nil,nil]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.Value{}, document.Value{}),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},

				// composite indexes must have at least have one value if typed
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, 0]",
					indexTypes: []document.ValueType{0, 0, 0},
					pivot:      values(document.NewIntegerValue(0), document.NewEmptyValue(document.IntegerValue), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, nil]",
					indexTypes: []document.ValueType{0, 0, 0},
					pivot:      values(document.NewIntegerValue(0), document.NewEmptyValue(document.IntegerValue), document.NewIntegerValue(0), document.Value{}),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: noCallEq,
					mustPanic:  true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(0), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// pivot [2, int] should filter out [2, not(int)]
				{name: "index=[any, untyped], vals=[int, int], noise=[int, blob], pivot=[2, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						// only [3, not(int)] is greater than [2, int], so it will appear anyway if we don't skip it
						if i < 3 {
							return values(document.NewIntegerValue(int64(i)), document.NewBoolValue(true))
						}

						return nil
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[any, untyped], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewBlobValue([]byte{byte('a' + uint8(i))})},
						)(val)
					},
					expectedCount: 3,
				},
				// partial pivot
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 10,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 6, // total * 2 - (noise + val = 2) * 2
				},
				// this is a tricky test, when we have multiple values but they share the first pivot element;
				// this is by definition a very implementation dependent test.
				{name: "index=[any, untyped], vals=[int, int], noise=int, bool], pivot=[int:0, int:0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(0), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBoolValue(true))
					},
					// [0, 0] > [0, true] but [1, true] > [0, 0] so we will see some bools in the results
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						if i%2 == 0 {
							i = i / 2
							requireIdxEncodedEq(t,
								encValue{false, document.NewIntegerValue(int64(i))},
								encValue{false, document.NewIntegerValue(int64(i + 1))},
							)(val)
						}
					},
					expectedCount: 9, // 10 elements, but pivot skipped the initial [0, true]
				},
				// index typed
				{name: "index=[int, int], vals=[int, int], pivot=[0, 0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(0), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2, 0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[int, blob], vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []document.ValueType{document.IntegerValue, document.BlobValue},
					pivot:      values(document.NewIntegerValue(2), document.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewBlobValue([]byte{byte('a' + uint8(i))})},
						)(val)
					},
					expectedCount: 3,
				},
				// partial pivot
				{name: "index=[int, int], vals=[int, int], pivot=[0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},

				// documents --------------------------------------------------
				{name: "index=[any, any], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						document.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[document, int], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []document.ValueType{document.DocumentValue, document.IntegerValue},
					pivot: values(
						document.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},

				// arrays -----------------------------------------------------
				{name: "index=[any, any], vals=[int[], int], pivot=[]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, any], vals=[int[], int[]], pivot=[[2,2], [3,3]]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						testutil.MakeArrayValue(t, 3, 3),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							testutil.MakeArrayValue(t, i+1, i+1),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, testutil.MakeArrayValue(t, i+1, i+1)},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[array, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []document.ValueType{document.ArrayValue, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i += 2
						requireIdxEncodedEq(t,
							encValue{true, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, unique, test.indexTypes...)
					defer cleanup()

					for i := 0; i < total; i++ {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
						if test.noise != nil {
							v := test.noise(i)
							if v != nil {
								require.NoError(t, idx.Set(test.noise(i), []byte{'a' + byte(i)}))
							}
						}
					}

					var i uint8
					var count int
					fn := func() error {
						return idx.AscendGreaterOrEqual(test.pivot, func(val, rid []byte) error {
							test.expectedEq(t, i, rid, val)
							i++
							count++
							return nil
						})
					}
					if test.mustPanic {
						// let's avoid panicking because expectedEq wasn't defined, which would
						// be a false positive.
						if test.expectedEq == nil {
							test.expectedEq = func(t *testing.T, i uint8, key, val []byte) {}
						}
						require.Panics(t, func() { _ = fn() })
					} else {
						err := fn()
						require.NoError(t, err)
						require.Equal(t, test.expectedCount, count)
					}
				})
			}
		})
	}
}

func TestIndexDescendLessOrEqual(t *testing.T) {
	for _, unique := range []bool{true, false} {
		text := fmt.Sprintf("Unique: %v, ", unique)

		t.Run(text+"Should not iterate if index is empty", func(t *testing.T) {
			idx, cleanup := getIndex(t, unique)
			defer cleanup()

			i := 0
			err := idx.AscendGreaterOrEqual(values(document.NewEmptyValue(document.IntegerValue)), func(val, key []byte) error {
				i++
				return errors.New("should not iterate")
			})
			require.NoError(t, err)
			require.Equal(t, 0, i)
		})

		t.Run(text+"Should iterate through documents in order, ", func(t *testing.T) {
			noiseBlob := func(i int) []document.Value {
				t.Helper()
				return []document.Value{document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10))}
			}
			noiseInts := func(i int) []document.Value {
				t.Helper()
				return []document.Value{document.NewIntegerValue(int64(i))}
			}

			noCallEq := func(t *testing.T, i uint8, key []byte, val []byte) {
				require.Fail(t, "equality test should not be called here")
			}

			// the following tests will use that constant to determine how many values needs to be inserted
			// with the value and noise generators.
			total := 5

			tests := []struct {
				name string
				// the index type(s) that is being used
				indexTypes []document.ValueType
				// the pivot, typed or not used to iterate
				pivot database.Pivot
				// the generator for the values that are being indexed
				val func(i int) []document.Value
				// the generator for the noise values that are being indexed
				noise func(i int) []document.Value
				// the function to compare the key/value that the iteration yields
				expectedEq func(t *testing.T, i uint8, key []byte, val []byte)
				// the total count of iteration that should happen
				expectedCount int
				mustPanic     bool
			}{
				// integers ---------------------------------------------------
				{name: "index=any, vals=integers, pivot=integer",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireEqualBinary(t, testutil.MakeArrayValue(t, int64(i)), val)
					},
					expectedCount: 5,
				},
				{name: "index=integer, vals=integers, pivot=integer",
					indexTypes: []document.ValueType{document.IntegerValue},
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=integers, pivot=integer:2",
					indexTypes: nil,
					pivot:      values(document.NewIntegerValue(2)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:      noiseBlob,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=integers, pivot=integer:-10",
					indexTypes:    nil,
					pivot:         values(document.NewIntegerValue(-10)),
					val:           func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					noise:         noiseBlob,
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=integer, vals=integers, pivot=integer:2",
					indexTypes: []document.ValueType{document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2)),
					val:        func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=integer, vals=integers, pivot=double",
					indexTypes:    []document.ValueType{document.IntegerValue},
					pivot:         values(document.NewEmptyValue(document.DoubleValue)),
					val:           func(i int) []document.Value { return values(document.NewIntegerValue(int64(i))) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// doubles ----------------------------------------------------
				{name: "index=any, vals=doubles, pivot=double",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.DoubleValue)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=doubles, pivot=double:1.8",
					indexTypes: nil,
					pivot:      values(document.NewDoubleValue(1.8)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 2,
				},
				{name: "index=double, vals=doubles, pivot=double:1.8",
					indexTypes: []document.ValueType{document.DoubleValue},
					pivot:      values(document.NewDoubleValue(1.8)),
					val:        func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewDoubleValue(float64(i) + float64(i)/2)},
						)(val)
					},
					expectedCount: 2,
				},
				{name: "index=any, vals=doubles, pivot=double:-10.8",
					indexTypes:    nil,
					pivot:         values(document.NewDoubleValue(-10.8)),
					val:           func(i int) []document.Value { return values(document.NewDoubleValue(float64(i) + float64(i)/2)) },
					expectedEq:    noCallEq,
					expectedCount: 0,
				},

				// text -------------------------------------------------------
				{name: "index=any, vals=text pivot=text",
					indexTypes: nil,
					pivot:      values(document.NewEmptyValue(document.TextValue)),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)

					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('2')",
					indexTypes: nil,
					pivot:      values(document.NewTextValue("2")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=any, vals=text, pivot=text('')",
					indexTypes: nil,
					pivot:      values(document.NewTextValue("")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=any, vals=text, pivot=text('foo')",
					indexTypes: nil,
					pivot:      values(document.NewTextValue("foo")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					noise:      noiseInts,
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{false, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=text, vals=text, pivot=text('2')",
					indexTypes: []document.ValueType{document.TextValue},
					pivot:      values(document.NewTextValue("2")),
					val:        func(i int) []document.Value { return values(document.NewTextValue(strconv.Itoa(i))) },
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						require.Equal(t, []byte{'a' + i}, key)
						requireIdxEncodedEq(t,
							encValue{true, document.NewTextValue(strconv.Itoa(int(i)))},
						)(val)
					},
					expectedCount: 3,
				},
				// composite --------------------------------------------------
				// composite indexes can have empty pivot values to iterate on the whole indexed data
				{name: "index=[any, untyped], vals=[int, int], pivot=[nil,nil]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.Value{}, document.Value{}),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				// composite indexes cannot have values with type past the first element
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, int, 0]",
					indexTypes: []document.ValueType{0, 0, 0},
					pivot:      values(document.NewIntegerValue(0), document.NewEmptyValue(document.IntegerValue), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)), document.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[int, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewEmptyValue(document.IntegerValue), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					mustPanic: true,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(0), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[5, 5]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(5), document.NewIntegerValue(5)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				// [0,1], [1,2], --[2,0]--,  [2,3], [3,4], [4,5]
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, 0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 2,
				},
				// [0,1], [1,2], [2,3], --[2,int]--, [3,4], [4,5]
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// pivot [2, int] should filter out [2, not(int)]
				// [0,1], [1,2], [2,3], --[2,int]--, [2, text], [3,4], [3,text], [4,5], [4,text]
				{name: "index=[any, untyped], vals=[int, int], noise=[int, text], pivot=[2, int]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewEmptyValue(document.IntegerValue)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						if i > 1 {
							return values(document.NewIntegerValue(int64(i)), document.NewTextValue("foo"))
						}

						return nil
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// a more subtle case
				{name: "index=[any, untyped], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2), document.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []document.Value {
						return values(
							document.NewIntegerValue(int64(i)),
							document.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)
					},
					noise: func(i int) []document.Value {
						return values(
							document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
							document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{false, document.NewBlobValue([]byte{byte('a' + uint8(i))})},
						)(val)
					},
					expectedCount: 2,
				},
				// only one of the indexed value is typed
				{name: "index=[any, blob], vals=[int, blob], noise=[blob, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []document.ValueType{0, document.BlobValue},
					pivot:      values(document.NewIntegerValue(2), document.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []document.Value {
						return values(
							document.NewIntegerValue(int64(i)),
							document.NewBlobValue([]byte{byte('a' + uint8(i))}),
						)
					},
					noise: func(i int) []document.Value {
						return values(
							document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
							document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							encValue{false, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewBlobValue([]byte{byte('a' + uint8(i))})},
						)(val)
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[0]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 2, // [0] is "equal" to [0, 1] and [0, "1"]
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[5]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(5)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 10,
				},
				{name: "index=[any, untyped], vals=[int, int], noise=[blob, blob], pivot=[2]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(document.NewIntegerValue(2)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					noise: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue(strconv.AppendInt(nil, int64(i), 10)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						// let's not try to match, it's not important
					},
					expectedCount: 6, // total * 2 - (noise + val = 2) * 2
				},
				// index typed
				{name: "index=[int, int], vals=[int, int], pivot=[0, 0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(0), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq:    noCallEq,
					expectedCount: 0,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[5, 6]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(5), document.NewIntegerValue(6)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[int, int], vals=[int, int], pivot=[2, 0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2), document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 2,
				},
				// a more subtle case
				{name: "index=[int, blob], vals=[int, blob], pivot=[2, 'a']", // pivot is [2, a] but value is [2, c] but that must work anyway
					indexTypes: []document.ValueType{document.IntegerValue, document.BlobValue},
					pivot:      values(document.NewIntegerValue(2), document.NewBlobValue([]byte{byte('a')})),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewBlobValue([]byte{byte('a' + uint8(i))}))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 3
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewBlobValue([]byte{byte('a' + uint8(i))})},
						)(val)
					},
					expectedCount: 2,
				},
				// partial pivot
				{name: "index=[int, int], vals=[int, int], pivot=[0]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(0)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 4
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 1,
				},
				// [0,1], [1,2], [2,3], --[2]--, [3,4], [4,5]
				{name: "index=[int, int], vals=[int, int], pivot=[2]",
					indexTypes: []document.ValueType{document.IntegerValue, document.IntegerValue},
					pivot:      values(document.NewIntegerValue(2)),
					val: func(i int) []document.Value {
						return values(document.NewIntegerValue(int64(i)), document.NewIntegerValue(int64(i+1)))
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewIntegerValue(int64(i))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				// documents --------------------------------------------------
				{name: "index=[any, any], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						document.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{false, document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`))},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[document, int], vals=[doc, int], pivot=[{a:2}, 3]",
					indexTypes: []document.ValueType{document.DocumentValue, document.IntegerValue},
					pivot: values(
						document.NewDocumentValue(testutil.MakeDocument(t, `{"a":2}`)),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(i)+`}`)),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{true, document.NewDocumentValue(testutil.MakeDocument(t, `{"a":`+strconv.Itoa(int(i))+`}`))},
							encValue{true, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},

				// arrays -----------------------------------------------------
				{name: "index=[any, any], vals=[int[], int], pivot=[]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot:      values(),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 5,
				},
				{name: "index=[any, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[any, any], vals=[int[], int[]], pivot=[[2,2], [3,3]]",
					indexTypes: []document.ValueType{document.AnyType, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						testutil.MakeArrayValue(t, 3, 3),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							testutil.MakeArrayValue(t, i+1, i+1),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{false, testutil.MakeArrayValue(t, i, i)},
							encValue{false, testutil.MakeArrayValue(t, i+1, i+1)},
						)(val)
					},
					expectedCount: 3,
				},
				{name: "index=[array, any], vals=[int[], int], pivot=[[2,2], 3]",
					indexTypes: []document.ValueType{document.ArrayValue, document.AnyType},
					pivot: values(
						testutil.MakeArrayValue(t, 2, 2),
						document.NewIntegerValue(int64(3)),
					),
					val: func(i int) []document.Value {
						return values(
							testutil.MakeArrayValue(t, i, i),
							document.NewIntegerValue(int64(i+1)),
						)
					},
					expectedEq: func(t *testing.T, i uint8, key []byte, val []byte) {
						i -= 2
						requireIdxEncodedEq(t,
							encValue{true, testutil.MakeArrayValue(t, i, i)},
							encValue{false, document.NewIntegerValue(int64(i + 1))},
						)(val)
					},
					expectedCount: 3,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					idx, cleanup := getIndex(t, unique, test.indexTypes...)
					defer cleanup()

					for i := 0; i < total; i++ {
						require.NoError(t, idx.Set(test.val(i), []byte{'a' + byte(i)}))
						if test.noise != nil {
							v := test.noise(i)
							if v != nil {
								require.NoError(t, idx.Set(test.noise(i), []byte{'a' + byte(i)}))
							}
						}
					}

					var i uint8
					var count int

					fn := func() error {
						t.Helper()
						return idx.DescendLessOrEqual(test.pivot, func(val, rid []byte) error {
							test.expectedEq(t, uint8(total-1)-i, rid, val)
							i++
							count++
							return nil
						})
					}
					if test.mustPanic {
						// let's avoid panicking because expectedEq wasn't defined, which would
						// be a false positive.
						if test.expectedEq == nil {
							test.expectedEq = func(t *testing.T, i uint8, key, val []byte) {}
						}
						require.Panics(t, func() {
							_ = fn()
						})
					} else {
						err := fn()
						require.NoError(t, err)
						require.Equal(t, test.expectedCount, count)
					}
				})
			}
		})
	}
}

// BenchmarkIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx, cleanup := getIndex(b, false)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					idx.Set(values(document.NewTextValue(k)), []byte(k))
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := getIndex(b, false)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(document.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.AscendGreaterOrEqual(values(document.NewEmptyValue(document.TextValue)), func(_, _ []byte) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}

// BenchmarkCompositeIndexSet benchmarks the Set method with 1, 10, 1000 and 10000 successive insertions.
func BenchmarkCompositeIndexSet(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				idx, cleanup := getIndex(b, false, document.TextValue, document.TextValue)

				b.StartTimer()
				for j := 0; j < size; j++ {
					k := fmt.Sprintf("name-%d", j)
					idx.Set(values(document.NewTextValue(k), document.NewTextValue(k)), []byte(k))
				}
				b.StopTimer()
				cleanup()
			}
		})
	}
}

// BenchmarkCompositeIndexIteration benchmarks the iterarion of a cursor with 1, 10, 1000 and 10000 items.
func BenchmarkCompositeIndexIteration(b *testing.B) {
	for size := 10; size <= 10000; size *= 10 {
		b.Run(fmt.Sprintf("%.05d", size), func(b *testing.B) {
			idx, cleanup := getIndex(b, false, document.AnyType, document.AnyType)
			defer cleanup()

			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("name-%d", i))
				_ = idx.Set(values(document.NewTextValue(string(k)), document.NewTextValue(string(k))), k)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.AscendGreaterOrEqual(values(document.NewTextValue(""), document.NewTextValue("")), func(_, _ []byte) error {
					return nil
				})
			}
			b.StopTimer()
		})
	}
}
