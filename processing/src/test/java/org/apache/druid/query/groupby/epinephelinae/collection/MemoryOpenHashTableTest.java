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

package org.apache.druid.query.groupby.epinephelinae.collection;

import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class MemoryOpenHashTableTest
{
  @Test
  public void testMemoryNeeded()
  {
    Assert.assertEquals(512, MemoryOpenHashTable.memoryNeeded(128, 4));
  }

  @Test
  public void testEmptyTable()
  {
    final MemoryOpenHashTable table = createTable(8, .75, 4, 4);

    Assert.assertEquals(8, table.numBuckets());
    Assert.assertEquals(72, table.memory().getCapacity());
    Assert.assertEquals(9, table.bucketSize());
    assertEqualsMap(ImmutableMap.of(), table);
  }

  @Test
  public void testInsertRepeatedKeys()
  {
    final MemoryOpenHashTable table = createTable(8, .7, Integer.BYTES, Integer.BYTES);
    final WritableMemory keyMemory = WritableMemory.allocate(Integer.BYTES);

    // Insert the following keys repeatedly.
    final int[] keys = {0, 1, 2};

    for (int i = 0; i < 3; i++) {
      for (int key : keys) {
        // Find bucket for key.
        keyMemory.putInt(0, key);
        int bucket = table.findBucket(HashTableUtils.hashMemory(keyMemory, 0, Integer.BYTES), keyMemory, 0);

        if (bucket < 0) {
          Assert.assertTrue(table.canInsertNewBucket());
          bucket = -(bucket + 1);
          table.initBucket(bucket, keyMemory, 0);
          final int valuePosition = table.bucketMemoryPosition(bucket) + table.bucketValueOffset();

          // Initialize to zero.
          table.memory().putInt(valuePosition, 0);
        }

        // Add the key.
        final int valuePosition = table.bucketMemoryPosition(bucket) + table.bucketValueOffset();
        table.memory().putInt(
            valuePosition,
            table.memory().getInt(valuePosition) + key
        );
      }
    }

    final Map<ByteBuffer, ByteBuffer> expectedMap = new HashMap<>();
    expectedMap.put(expectedKey(0), expectedValue(0));
    expectedMap.put(expectedKey(1), expectedValue(3));
    expectedMap.put(expectedKey(2), expectedValue(6));

    assertEqualsMap(expectedMap, table);
  }

  @Test
  public void testInsertDifferentKeysUntilFull()
  {
    final MemoryOpenHashTable table = createTable(256, .999, Integer.BYTES, Integer.BYTES);
    final Map<ByteBuffer, ByteBuffer> expectedMap = new HashMap<>();

    int key = 0;
    while (table.canInsertNewBucket()) {
      final int value = Integer.MAX_VALUE - key;

      // Find bucket for key (which should not already exist).
      final int bucket = findAndInitBucket(table, key);
      Assert.assertTrue("bucket < 0 for key " + key, bucket < 0);

      // Insert bucket and write value.
      writeValueToBucket(table, -(bucket + 1), value);
      expectedMap.put(expectedKey(key), expectedValue(value));

      key += 7;
    }

    // This table should fill up at 255 elements (256 buckets, .999 load factor)
    Assert.assertEquals("expected size", 255, table.size());
    assertEqualsMap(expectedMap, table);
  }

  @Test
  public void testCopyTo()
  {
    final MemoryOpenHashTable table1 = createTable(64, .7, Integer.BYTES, Integer.BYTES);
    final MemoryOpenHashTable table2 = createTable(128, .7, Integer.BYTES, Integer.BYTES);

    final Int2IntMap expectedMap = new Int2IntOpenHashMap();
    expectedMap.put(0, 1);
    expectedMap.put(-1, 2);
    expectedMap.put(111, 123);
    expectedMap.put(Integer.MAX_VALUE, Integer.MIN_VALUE);
    expectedMap.put(Integer.MIN_VALUE, Integer.MAX_VALUE);

    // Populate table1.
    for (Int2IntMap.Entry entry : expectedMap.int2IntEntrySet()) {
      final int bucket = findAndInitBucket(table1, entry.getIntKey());
      Assert.assertTrue("bucket < 0 for key " + entry.getIntKey(), bucket < 0);
      writeValueToBucket(table1, -(bucket + 1), entry.getIntValue());
    }

    // Copy to table2.
    table1.copyTo(table2, ((oldBucket, newBucket, oldTable, newTable) -> {
      Assert.assertSame(table1, oldTable);
      Assert.assertSame(table2, newTable);
    }));

    // Compute expected map to compare these tables to.
    final Map<ByteBuffer, ByteBuffer> expectedByteBufferMap =
        expectedMap.int2IntEntrySet()
                   .stream()
                   .collect(
                       Collectors.toMap(
                           entry -> expectedKey(entry.getIntKey()),
                           entry -> expectedValue(entry.getIntValue())
                       )
                   );

    assertEqualsMap(expectedByteBufferMap, table1);
    assertEqualsMap(expectedByteBufferMap, table2);
  }

  @Test
  public void testClear()
  {
    final MemoryOpenHashTable table = createTable(64, .7, Integer.BYTES, Integer.BYTES);

    final Int2IntMap expectedMap = new Int2IntOpenHashMap();
    expectedMap.put(0, 1);
    expectedMap.put(-1, 2);

    // Populate table.
    for (Int2IntMap.Entry entry : expectedMap.int2IntEntrySet()) {
      final int bucket = findAndInitBucket(table, entry.getIntKey());
      Assert.assertTrue("bucket < 0 for key " + entry.getIntKey(), bucket < 0);
      writeValueToBucket(table, -(bucket + 1), entry.getIntValue());
    }

    // Compute expected map to compare these tables to.
    final Map<ByteBuffer, ByteBuffer> expectedByteBufferMap =
        expectedMap.int2IntEntrySet()
                   .stream()
                   .collect(
                       Collectors.toMap(
                           entry -> expectedKey(entry.getIntKey()),
                           entry -> expectedValue(entry.getIntValue())
                       )
                   );

    assertEqualsMap(expectedByteBufferMap, table);

    // Clear and verify.
    table.clear();

    assertEqualsMap(ImmutableMap.of(), table);
  }

  /**
   * Finds the bucket for the provided key using {@link MemoryOpenHashTable#findBucket} and initializes it if empty
   * using {@link MemoryOpenHashTable#initBucket}. Same return value as {@link MemoryOpenHashTable#findBucket}.
   */
  private static int findAndInitBucket(final MemoryOpenHashTable table, final int key)
  {
    final int keyMemoryPosition = 1; // Helps verify that offsets work
    final WritableMemory keyMemory = WritableMemory.allocate(Integer.BYTES + 1);

    keyMemory.putInt(keyMemoryPosition, key);

    final int bucket = table.findBucket(
        HashTableUtils.hashMemory(keyMemory, keyMemoryPosition, Integer.BYTES),
        keyMemory,
        keyMemoryPosition
    );

    if (bucket < 0) {
      table.initBucket(-(bucket + 1), keyMemory, keyMemoryPosition);
    }

    return bucket;
  }

  /**
   * Writes a value to a bucket. The bucket must have already been initialized by calling
   * {@link MemoryOpenHashTable#initBucket}.
   */
  private static void writeValueToBucket(final MemoryOpenHashTable table, final int bucket, final int value)
  {
    final int valuePosition = table.bucketMemoryPosition(bucket) + table.bucketValueOffset();
    table.memory().putInt(valuePosition, value);
  }

  /**
   * Returns a set of key, value pairs from the provided table. Uses the table's {@link MemoryOpenHashTable#bucketIterator()}
   * method.
   */
  private static Set<ByteBufferPair> pairSet(final MemoryOpenHashTable table)
  {
    final Set<ByteBufferPair> retVal = new HashSet<>();

    final IntIterator bucketIterator = table.bucketIterator();

    while (bucketIterator.hasNext()) {
      final int bucket = bucketIterator.nextInt();
      final ByteBuffer entryBuffer = table.memory().getByteBuffer().duplicate();
      entryBuffer.position(table.bucketMemoryPosition(bucket));
      entryBuffer.limit(entryBuffer.position() + table.bucketSize());

      // Must copy since we're materializing, and the buffer will get reused.
      final ByteBuffer keyBuffer = ByteBuffer.allocate(table.keySize());
      final ByteBuffer keyDup = entryBuffer.duplicate();
      final int keyPosition = keyDup.position() + table.bucketKeyOffset();
      keyDup.position(keyPosition);
      keyDup.limit(keyPosition + table.keySize());
      keyBuffer.put(keyDup);
      keyBuffer.position(0);

      final ByteBuffer valueBuffer = ByteBuffer.allocate(table.valueSize());
      final ByteBuffer valueDup = entryBuffer.duplicate();
      final int valuePosition = valueDup.position() + table.bucketValueOffset();
      valueDup.position(valuePosition);
      valueDup.limit(valuePosition + table.valueSize());
      valueBuffer.put(valueDup);
      valueBuffer.position(0);

      retVal.add(new ByteBufferPair(keyBuffer, valueBuffer));
    }

    return retVal;
  }

  private static MemoryOpenHashTable createTable(
      final int numBuckets,
      final double loadFactor,
      final int keySize,
      final int valueSize
  )
  {
    final int maxSize = (int) Math.floor(numBuckets * loadFactor);

    final ByteBuffer buffer = ByteBuffer.allocate(
        MemoryOpenHashTable.memoryNeeded(
            numBuckets,
            MemoryOpenHashTable.bucketSize(keySize, valueSize)
        )
    );

    // Scribble garbage to make sure that we don't depend on the buffer being clear.
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.put(i, (byte) ThreadLocalRandom.current().nextInt());
    }

    return new MemoryOpenHashTable(
        WritableMemory.wrap(buffer, ByteOrder.nativeOrder()),
        numBuckets,
        maxSize,
        keySize,
        valueSize
    );
  }

  private static ByteBuffer expectedKey(final int key)
  {
    return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder()).putInt(0, key);
  }

  private static ByteBuffer expectedValue(final int value)
  {
    return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder()).putInt(0, value);
  }

  private static void assertEqualsMap(final Map<ByteBuffer, ByteBuffer> expected, final MemoryOpenHashTable actual)
  {
    Assert.assertEquals("size", expected.size(), actual.size());
    Assert.assertEquals(
        "entries",
        expected.entrySet()
                .stream()
                .map(entry -> new ByteBufferPair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet()),
        pairSet(actual)
    );
  }

  private static class ByteBufferPair extends Pair<ByteBuffer, ByteBuffer>
  {
    public ByteBufferPair(ByteBuffer lhs, ByteBuffer rhs)
    {
      super(lhs, rhs);
    }

    @Override
    public String toString()
    {
      final byte[] lhsBytes = new byte[lhs.remaining()];
      lhs.duplicate().get(lhsBytes);

      final byte[] rhsBytes = new byte[rhs.remaining()];
      rhs.duplicate().get(rhsBytes);

      return "ByteBufferPair{" +
             "lhs=" + Arrays.toString(lhsBytes) +
             ", rhs=" + Arrays.toString(rhsBytes) +
             '}';
    }
  }
}
