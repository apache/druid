/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.topn.vector;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TopNVectorColumnSelectorTest
{
  private static final int NUM_ROWS = 4;

  // -- LongTopNVectorColumnSelector --

  @Test
  public void testLongKeySizeIs8()
  {
    final LongTopNVectorColumnSelector sel = new LongTopNVectorColumnSelector(mockLongSelector(new long[]{1, 2, 3, 4}));
    Assertions.assertEquals(Long.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testLongWriteKeysBulkPath()
  {
    final long[] longs = {10L, 20L, 30L, 40L};
    final LongTopNVectorColumnSelector sel = new LongTopNVectorColumnSelector(mockLongSelector(longs));
    final WritableMemory keySpace = WritableMemory.allocate(Long.BYTES * NUM_ROWS);

    sel.writeKeys(keySpace, Long.BYTES, 0, 0, NUM_ROWS);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assertions.assertEquals(longs[i], keySpace.getLong((long) i * Long.BYTES));
    }
  }

  @Test
  public void testLongWriteKeysStridedPath()
  {
    final long[] longs = {10L, 20L, 30L, 40L};
    final LongTopNVectorColumnSelector sel = new LongTopNVectorColumnSelector(mockLongSelector(longs));
    // keySize > Long.BYTES forces the strided path
    final int keySize = Long.BYTES + 1;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * NUM_ROWS);

    sel.writeKeys(keySpace, keySize, 0, 0, NUM_ROWS);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assertions.assertEquals(longs[i], keySpace.getLong((long) i * keySize));
    }
  }

  @Test
  public void testLongGetDimensionValue()
  {
    final LongTopNVectorColumnSelector sel = new LongTopNVectorColumnSelector(mockLongSelector(new long[]{42L}));
    final WritableMemory mem = WritableMemory.allocate(Long.BYTES);
    mem.putLong(0, 42L);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals(42L, sel.getDimensionValue(ptr, 0));
  }

  // -- NullableLongTopNVectorColumnSelector --

  @Test
  public void testNullableLongKeySizeIs9()
  {
    final NullableLongTopNVectorColumnSelector sel =
        new NullableLongTopNVectorColumnSelector(mockLongSelector(new long[]{1}));
    Assertions.assertEquals(Byte.BYTES + Long.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testNullableLongWriteKeysWithNullVector()
  {
    final long[] longs = {0L, 99L};
    final boolean[] nulls = {true, false};
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getLongVector()).thenReturn(longs);
    Mockito.when(mock.getNullVector()).thenReturn(nulls);

    final NullableLongTopNVectorColumnSelector sel = new NullableLongTopNVectorColumnSelector(mock);
    final int keySize = Byte.BYTES + Long.BYTES;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * 2);

    sel.writeKeys(keySpace, keySize, 0, 0, 2);

    Assertions.assertEquals(TypeStrategies.IS_NULL_BYTE, keySpace.getByte(0));
    Assertions.assertEquals(TypeStrategies.IS_NOT_NULL_BYTE, keySpace.getByte(keySize));
    Assertions.assertEquals(99L, keySpace.getLong(keySize + 1));
  }

  @Test
  public void testNullableLongWriteKeysWithoutNullVector()
  {
    final long[] longs = {7L, 8L};
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getLongVector()).thenReturn(longs);
    Mockito.when(mock.getNullVector()).thenReturn(null);

    final NullableLongTopNVectorColumnSelector sel = new NullableLongTopNVectorColumnSelector(mock);
    final int keySize = Byte.BYTES + Long.BYTES;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * 2);

    sel.writeKeys(keySpace, keySize, 0, 0, 2);

    Assertions.assertEquals(TypeStrategies.IS_NOT_NULL_BYTE, keySpace.getByte(0));
    Assertions.assertEquals(7L, keySpace.getLong(1));
    Assertions.assertEquals(TypeStrategies.IS_NOT_NULL_BYTE, keySpace.getByte(keySize));
    Assertions.assertEquals(8L, keySpace.getLong(keySize + 1));
  }

  @Test
  public void testNullableLongGetDimensionValueNull()
  {
    final NullableLongTopNVectorColumnSelector sel =
        new NullableLongTopNVectorColumnSelector(mockLongSelector(new long[]{0}));
    final int keySize = Byte.BYTES + Long.BYTES;
    final WritableMemory mem = WritableMemory.allocate(keySize);
    mem.putByte(0, TypeStrategies.IS_NULL_BYTE);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertNull(sel.getDimensionValue(ptr, 0));
  }

  @Test
  public void testNullableLongGetDimensionValueNonNull()
  {
    final NullableLongTopNVectorColumnSelector sel =
        new NullableLongTopNVectorColumnSelector(mockLongSelector(new long[]{0}));
    final int keySize = Byte.BYTES + Long.BYTES;
    final WritableMemory mem = WritableMemory.allocate(keySize);
    mem.putByte(0, TypeStrategies.IS_NOT_NULL_BYTE);
    mem.putLong(1, 55L);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals(55L, sel.getDimensionValue(ptr, 0));
  }

  // -- DoubleTopNVectorColumnSelector --

  @Test
  public void testDoubleKeySizeIs8()
  {
    final DoubleTopNVectorColumnSelector sel =
        new DoubleTopNVectorColumnSelector(mockDoubleSelector(new double[]{1.0}));
    Assertions.assertEquals(Double.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testDoubleWriteKeysBulkPath()
  {
    final double[] doubles = {1.1, 2.2, 3.3};
    final DoubleTopNVectorColumnSelector sel = new DoubleTopNVectorColumnSelector(mockDoubleSelector(doubles));
    final WritableMemory keySpace = WritableMemory.allocate(Double.BYTES * 3);

    sel.writeKeys(keySpace, Double.BYTES, 0, 0, 3);

    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(doubles[i], keySpace.getDouble((long) i * Double.BYTES), 0.0);
    }
  }

  @Test
  public void testDoubleGetDimensionValue()
  {
    final DoubleTopNVectorColumnSelector sel =
        new DoubleTopNVectorColumnSelector(mockDoubleSelector(new double[]{0}));
    final WritableMemory mem = WritableMemory.allocate(Double.BYTES);
    mem.putDouble(0, 3.14);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals(3.14, (double) sel.getDimensionValue(ptr, 0), 0.0);
  }

  // -- NullableDoubleTopNVectorColumnSelector --

  @Test
  public void testNullableDoubleKeySizeIs9()
  {
    final NullableDoubleTopNVectorColumnSelector sel =
        new NullableDoubleTopNVectorColumnSelector(mockDoubleSelector(new double[]{1.0}));
    Assertions.assertEquals(Byte.BYTES + Double.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testNullableDoubleWriteKeysWithNullVector()
  {
    final double[] doubles = {0.0, 2.5};
    final boolean[] nulls = {true, false};
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getDoubleVector()).thenReturn(doubles);
    Mockito.when(mock.getNullVector()).thenReturn(nulls);

    final NullableDoubleTopNVectorColumnSelector sel = new NullableDoubleTopNVectorColumnSelector(mock);
    final int keySize = Byte.BYTES + Double.BYTES;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * 2);

    sel.writeKeys(keySpace, keySize, 0, 0, 2);

    Assertions.assertEquals(TypeStrategies.IS_NULL_BYTE, keySpace.getByte(0));
    Assertions.assertEquals(TypeStrategies.IS_NOT_NULL_BYTE, keySpace.getByte(keySize));
    Assertions.assertEquals(2.5, keySpace.getDouble(keySize + 1), 0.0);
  }

  @Test
  public void testNullableDoubleGetDimensionValueNull()
  {
    final NullableDoubleTopNVectorColumnSelector sel =
        new NullableDoubleTopNVectorColumnSelector(mockDoubleSelector(new double[]{0}));
    final int keySize = Byte.BYTES + Double.BYTES;
    final WritableMemory mem = WritableMemory.allocate(keySize);
    mem.putByte(0, TypeStrategies.IS_NULL_BYTE);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertNull(sel.getDimensionValue(ptr, 0));
  }

  // -- FloatTopNVectorColumnSelector --

  @Test
  public void testFloatKeySizeIs4()
  {
    final FloatTopNVectorColumnSelector sel =
        new FloatTopNVectorColumnSelector(mockFloatSelector(new float[]{1.0f}));
    Assertions.assertEquals(Float.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testFloatWriteKeysBulkPath()
  {
    final float[] floats = {1.1f, 2.2f, 3.3f};
    final FloatTopNVectorColumnSelector sel = new FloatTopNVectorColumnSelector(mockFloatSelector(floats));
    final WritableMemory keySpace = WritableMemory.allocate(Float.BYTES * 3);

    sel.writeKeys(keySpace, Float.BYTES, 0, 0, 3);

    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(floats[i], keySpace.getFloat((long) i * Float.BYTES), 0.0f);
    }
  }

  @Test
  public void testFloatGetDimensionValue()
  {
    final FloatTopNVectorColumnSelector sel =
        new FloatTopNVectorColumnSelector(mockFloatSelector(new float[]{0}));
    final WritableMemory mem = WritableMemory.allocate(Float.BYTES);
    mem.putFloat(0, 1.5f);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals(1.5f, (float) sel.getDimensionValue(ptr, 0), 0.0f);
  }

  // -- NullableFloatTopNVectorColumnSelector --

  @Test
  public void testNullableFloatKeySizeIs5()
  {
    final NullableFloatTopNVectorColumnSelector sel =
        new NullableFloatTopNVectorColumnSelector(mockFloatSelector(new float[]{1.0f}));
    Assertions.assertEquals(Byte.BYTES + Float.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testNullableFloatWriteKeysWithNullVector()
  {
    final float[] floats = {0.0f, 9.9f};
    final boolean[] nulls = {true, false};
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getFloatVector()).thenReturn(floats);
    Mockito.when(mock.getNullVector()).thenReturn(nulls);

    final NullableFloatTopNVectorColumnSelector sel = new NullableFloatTopNVectorColumnSelector(mock);
    final int keySize = Byte.BYTES + Float.BYTES;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * 2);

    sel.writeKeys(keySpace, keySize, 0, 0, 2);

    Assertions.assertEquals(TypeStrategies.IS_NULL_BYTE, keySpace.getByte(0));
    Assertions.assertEquals(TypeStrategies.IS_NOT_NULL_BYTE, keySpace.getByte(keySize));
    Assertions.assertEquals(9.9f, keySpace.getFloat(keySize + 1), 0.0f);
  }

  @Test
  public void testNullableFloatGetDimensionValueNull()
  {
    final NullableFloatTopNVectorColumnSelector sel =
        new NullableFloatTopNVectorColumnSelector(mockFloatSelector(new float[]{0}));
    final int keySize = Byte.BYTES + Float.BYTES;
    final WritableMemory mem = WritableMemory.allocate(keySize);
    mem.putByte(0, TypeStrategies.IS_NULL_BYTE);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertNull(sel.getDimensionValue(ptr, 0));
  }

  @Test
  public void testNullableFloatGetDimensionValueNonNull()
  {
    final NullableFloatTopNVectorColumnSelector sel =
        new NullableFloatTopNVectorColumnSelector(mockFloatSelector(new float[]{0}));
    final int keySize = Byte.BYTES + Float.BYTES;
    final WritableMemory mem = WritableMemory.allocate(keySize);
    mem.putByte(0, TypeStrategies.IS_NOT_NULL_BYTE);
    mem.putFloat(1, 7.7f);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals(7.7f, (float) sel.getDimensionValue(ptr, 0), 0.0f);
  }

  // -- SingleValueStringTopNVectorColumnSelector --

  @Test
  public void testStringKeySizeIs4()
  {
    final SingleValueDimensionVectorSelector mock = Mockito.mock(SingleValueDimensionVectorSelector.class);
    final SingleValueStringTopNVectorColumnSelector sel = new SingleValueStringTopNVectorColumnSelector(mock);
    Assertions.assertEquals(Integer.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testStringWriteKeysBulkPath()
  {
    final int[] dictIds = {2, 0, 1, 3};
    final SingleValueDimensionVectorSelector mock = Mockito.mock(SingleValueDimensionVectorSelector.class);
    Mockito.when(mock.getRowVector()).thenReturn(dictIds);

    final SingleValueStringTopNVectorColumnSelector sel = new SingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory keySpace = WritableMemory.allocate(Integer.BYTES * NUM_ROWS);

    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, NUM_ROWS);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assertions.assertEquals(dictIds[i], keySpace.getInt((long) i * Integer.BYTES));
    }
  }

  @Test
  public void testStringWriteKeysStridedPath()
  {
    final int[] dictIds = {5, 3};
    final SingleValueDimensionVectorSelector mock = Mockito.mock(SingleValueDimensionVectorSelector.class);
    Mockito.when(mock.getRowVector()).thenReturn(dictIds);

    final SingleValueStringTopNVectorColumnSelector sel = new SingleValueStringTopNVectorColumnSelector(mock);
    // keySize > Integer.BYTES forces the strided path
    final int keySize = Integer.BYTES + 1;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * 2);

    sel.writeKeys(keySpace, keySize, 0, 0, 2);

    Assertions.assertEquals(5, keySpace.getInt(0));
    Assertions.assertEquals(3, keySpace.getInt(keySize));
  }

  @Test
  public void testStringGetDimensionValue()
  {
    final SingleValueDimensionVectorSelector mock = Mockito.mock(SingleValueDimensionVectorSelector.class);
    Mockito.when(mock.lookupName(2)).thenReturn("hello");

    final SingleValueStringTopNVectorColumnSelector sel = new SingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory mem = WritableMemory.allocate(Integer.BYTES);
    mem.putInt(0, 2);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals("hello", sel.getDimensionValue(ptr, 0));
  }

  @Test
  public void testStringGetValueCardinality()
  {
    final SingleValueDimensionVectorSelector mock = Mockito.mock(SingleValueDimensionVectorSelector.class);
    Mockito.when(mock.getValueCardinality()).thenReturn(42);

    final SingleValueStringTopNVectorColumnSelector sel = new SingleValueStringTopNVectorColumnSelector(mock);
    Assertions.assertEquals(42, sel.getValueCardinality());
  }

  // -- DictionaryBuildingSingleValueStringTopNVectorColumnSelector --

  @Test
  public void testDictBuildingKeySizeIs4()
  {
    final DictionaryBuildingSingleValueStringTopNVectorColumnSelector sel =
        new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(
            Mockito.mock(org.apache.druid.segment.vector.VectorObjectSelector.class)
        );
    Assertions.assertEquals(Integer.BYTES, sel.getGroupingKeySize());
  }

  @Test
  public void testDictBuildingWriteKeysBuildsDictionary()
  {
    final Object[] objects = {"foo", "bar", "foo", "baz"};
    final org.apache.druid.segment.vector.VectorObjectSelector mock =
        Mockito.mock(org.apache.druid.segment.vector.VectorObjectSelector.class);
    Mockito.when(mock.getObjectVector()).thenReturn(objects);

    final DictionaryBuildingSingleValueStringTopNVectorColumnSelector sel =
        new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory keySpace = WritableMemory.allocate(Integer.BYTES * 4);

    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, 4);

    // "foo" should get id 0, "bar" id 1, "baz" id 2
    final int fooId = keySpace.getInt(0);
    final int barId = keySpace.getInt(Integer.BYTES);
    final int baz = keySpace.getInt(Integer.BYTES * 3);

    Assertions.assertEquals(fooId, keySpace.getInt(Integer.BYTES * 2)); // second "foo" same id
    Assertions.assertNotEquals(fooId, barId);
    Assertions.assertNotEquals(fooId, baz);
    Assertions.assertNotEquals(barId, baz);
  }

  @Test
  public void testDictBuildingGetDimensionValue()
  {
    final Object[] objects = {"alpha", "beta"};
    final org.apache.druid.segment.vector.VectorObjectSelector mock =
        Mockito.mock(org.apache.druid.segment.vector.VectorObjectSelector.class);
    Mockito.when(mock.getObjectVector()).thenReturn(objects);

    final DictionaryBuildingSingleValueStringTopNVectorColumnSelector sel =
        new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory keySpace = WritableMemory.allocate(Integer.BYTES * 2);
    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, 2);

    // Look up the id that was assigned to "alpha"
    final int alphaId = keySpace.getInt(0);
    final WritableMemory mem = WritableMemory.allocate(Integer.BYTES);
    mem.putInt(0, alphaId);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertEquals("alpha", sel.getDimensionValue(ptr, 0));
  }

  @Test
  public void testDictBuildingResetClearsDictionary()
  {
    final Object[] objects = {"x"};
    final org.apache.druid.segment.vector.VectorObjectSelector mock =
        Mockito.mock(org.apache.druid.segment.vector.VectorObjectSelector.class);
    Mockito.when(mock.getObjectVector()).thenReturn(objects);

    final DictionaryBuildingSingleValueStringTopNVectorColumnSelector sel =
        new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory keySpace = WritableMemory.allocate(Integer.BYTES);
    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, 1);

    sel.reset();

    // After reset, writing the same value again should assign id 0 again (dictionary was cleared).
    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, 1);
    Assertions.assertEquals(0, keySpace.getInt(0));
  }

  @Test
  public void testDictBuildingNullValueRoundTrip()
  {
    final Object[] objects = {null, "y"};
    final org.apache.druid.segment.vector.VectorObjectSelector mock =
        Mockito.mock(org.apache.druid.segment.vector.VectorObjectSelector.class);
    Mockito.when(mock.getObjectVector()).thenReturn(objects);

    final DictionaryBuildingSingleValueStringTopNVectorColumnSelector sel =
        new DictionaryBuildingSingleValueStringTopNVectorColumnSelector(mock);
    final WritableMemory keySpace = WritableMemory.allocate(Integer.BYTES * 2);
    sel.writeKeys(keySpace, Integer.BYTES, 0, 0, 2);

    final int nullId = keySpace.getInt(0);
    final WritableMemory mem = WritableMemory.allocate(Integer.BYTES);
    mem.putInt(0, nullId);

    final MemoryPointer ptr = new MemoryPointer();
    ptr.set(mem, 0);

    Assertions.assertNull(sel.getDimensionValue(ptr, 0));
  }

  // -- helpers --

  private VectorValueSelector mockLongSelector(final long[] values)
  {
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getLongVector()).thenReturn(values);
    Mockito.when(mock.getNullVector()).thenReturn(null);
    return mock;
  }

  private VectorValueSelector mockDoubleSelector(final double[] values)
  {
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getDoubleVector()).thenReturn(values);
    Mockito.when(mock.getNullVector()).thenReturn(null);
    return mock;
  }

  private VectorValueSelector mockFloatSelector(final float[] values)
  {
    final VectorValueSelector mock = Mockito.mock(VectorValueSelector.class);
    Mockito.when(mock.getFloatVector()).thenReturn(values);
    Mockito.when(mock.getNullVector()).thenReturn(null);
    return mock;
  }
}
