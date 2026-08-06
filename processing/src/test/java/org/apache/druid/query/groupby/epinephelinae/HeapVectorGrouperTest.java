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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

public class HeapVectorGrouperTest
{
  private static final int KEY_SIZE = Integer.BYTES;
  private static final int AGG_SIZE = Long.BYTES;
  private static final int MAX_VECTOR_SIZE = 512;

  // -- structural / always-ok tests --

  @Test
  public void testAggregateVectorAlwaysReturnsOk()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    final WritableMemory keySpace = makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, 10);

    AggregateResult result = grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);
    Assertions.assertTrue(result.isOk());
  }

  @Test
  public void testCorrectNumberOfDistinctGroups()
  {
    final int distinctKeys = 7;
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    final WritableMemory keySpace = makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, distinctKeys);

    grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);

    Assertions.assertEquals(distinctKeys, countGroups(grouper));
  }

  @Test
  public void testSameKeyProducesSameGroupAcrossBatches()
  {
    final int distinctKeys = 3;
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    final WritableMemory keySpace = makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, distinctKeys);

    grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);
    grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);

    // Same keys across two batches → same group count
    Assertions.assertEquals(distinctKeys, countGroups(grouper));
  }

  @Test
  public void testGroupKeyRoundTrip()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    final WritableMemory keySpace = WritableMemory.allocate(KEY_SIZE * MAX_VECTOR_SIZE);
    // Single batch with keys 0..4
    for (int i = 0; i < MAX_VECTOR_SIZE; i++) {
      keySpace.putInt((long) i * KEY_SIZE, i % 5);
    }
    grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);

    // Iterator must produce exactly the 5 keys we inserted
    final Set<Integer> seen = new HashSet<>();
    try (CloseableIterator<Grouper.Entry<MemoryPointer>> iter = grouper.iterator()) {
      while (iter.hasNext()) {
        final Grouper.Entry<MemoryPointer> entry = iter.next();
        final int key = entry.getKey().memory().getInt(entry.getKey().position());
        seen.add(key);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    Assertions.assertEquals(Set.of(0, 1, 2, 3, 4), seen);
  }

  // -- buffer growth --

  @Test
  public void testBufferGrowsToAccommodateManyGroups()
  {
    // Use a large aggSize to force growth sooner
    final int aggSize = 1024;
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, aggSize);

    // Add enough distinct keys to require several doublings from the 4KB initial size
    int key = 0;
    final WritableMemory keySpace = WritableMemory.allocate(KEY_SIZE * MAX_VECTOR_SIZE);
    for (int batch = 0; batch < 20; batch++) {
      for (int i = 0; i < MAX_VECTOR_SIZE; i++) {
        keySpace.putInt((long) i * KEY_SIZE, key++);
      }
      final AggregateResult result = grouper.aggregateVector(keySpace, 0, MAX_VECTOR_SIZE);
      Assertions.assertTrue(result.isOk(), "batch " + batch + " must be ok");
    }
    // All 20 * MAX_VECTOR_SIZE distinct keys must be present
    Assertions.assertEquals(20 * MAX_VECTOR_SIZE, countGroups(grouper));
  }

  // -- reset / close --

  @Test
  public void testResetClearsAllGroups()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    grouper.aggregateVector(makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, 10), 0, MAX_VECTOR_SIZE);

    grouper.reset();

    Assertions.assertEquals(0, countGroups(grouper));
  }

  @Test
  public void testAfterResetNewGroupsCanBeAdded()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    grouper.aggregateVector(makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, 10), 0, MAX_VECTOR_SIZE);
    grouper.reset();

    grouper.aggregateVector(makeKeySpace(MAX_VECTOR_SIZE, KEY_SIZE, 3), 0, MAX_VECTOR_SIZE);
    Assertions.assertEquals(3, countGroups(grouper));
  }

  @Test
  public void testCloseCallsAggregatorsReset()
  {
    final AggregatorAdapters aggregators = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregators.spaceNeeded()).thenReturn(AGG_SIZE);

    final HeapVectorGrouper grouper = new HeapVectorGrouper(aggregators, KEY_SIZE);
    grouper.initVectorized(MAX_VECTOR_SIZE);
    grouper.close();

    Mockito.verify(aggregators, Mockito.times(1)).reset();
  }

  // -- initVectorized contracts --

  @Test
  public void testInitTwiceWithSameSizeIsNoOp()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    grouper.initVectorized(MAX_VECTOR_SIZE);
    // Must not throw — second call with same size is idempotent
    grouper.initVectorized(MAX_VECTOR_SIZE);
  }

  @Test
  public void testInitTwiceWithDifferentSizeThrows()
  {
    final HeapVectorGrouper grouper = makeGrouper(KEY_SIZE, AGG_SIZE);
    grouper.initVectorized(512);
    Assertions.assertThrows(ISE.class, () -> grouper.initVectorized(256));
  }

  // -- helpers --

  private HeapVectorGrouper makeGrouper(final int keySize, final int aggSize)
  {
    final AggregatorAdapters aggregators = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregators.spaceNeeded()).thenReturn(aggSize);

    final HeapVectorGrouper grouper = new HeapVectorGrouper(aggregators, keySize);
    grouper.initVectorized(MAX_VECTOR_SIZE);
    return grouper;
  }

  private WritableMemory makeKeySpace(final int numRows, final int keySize, final int distinctKeys)
  {
    final WritableMemory keySpace = WritableMemory.allocate(keySize * numRows);
    for (int i = 0; i < numRows; i++) {
      keySpace.putInt((long) i * keySize, i % distinctKeys);
    }
    return keySpace;
  }

  private int countGroups(final HeapVectorGrouper grouper)
  {
    int count = 0;
    try (CloseableIterator<Grouper.Entry<MemoryPointer>> iter = grouper.iterator()) {
      while (iter.hasNext()) {
        iter.next();
        count++;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return count;
  }
}
