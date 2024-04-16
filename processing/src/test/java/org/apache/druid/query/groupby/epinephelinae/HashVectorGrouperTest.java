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

import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class HashVectorGrouperTest
{
  @Test
  public void testCloseAggregatorAdaptorsShouldBeClosed()
  {
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[4096]);
    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        1024,
        aggregatorAdapters,
        Integer.MAX_VALUE,
        0.f,
        0
    );
    grouper.initVectorized(512);
    grouper.close();
    Mockito.verify(aggregatorAdapters, Mockito.times(2)).reset();
  }

  @Test
  public void testTableStartIsNotMemoryStartIfNotMaxSized()
  {
    final int maxVectorSize = 512;
    final int keySize = 4;
    final int bufferSize = 100 * 1024;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * maxVectorSize);
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        8,
        0.f,
        4
    );
    grouper.initVectorized(maxVectorSize);
    Assert.assertNotEquals(0, grouper.getTableStart());
  }

  @Test
  public void testTableStartIsNotMemoryStartIfIsMaxSized()
  {
    final int maxVectorSize = 512;
    final int keySize = 10000;
    final int bufferSize = 100 * 1024;
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        4,
        0.f,
        4
    );
    grouper.initVectorized(maxVectorSize);
    Assert.assertEquals(0, grouper.getTableStart());
  }

  @Test
  public void testGrowOnce()
  {
    final int maxVectorSize = 512;
    final int keySize = 4;
    final int aggSize = 8;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * maxVectorSize);

    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregatorAdapters.spaceNeeded()).thenReturn(aggSize);

    int startingNumBuckets = 4;
    int maxBuckets = 16;
    final int bufferSize = (keySize + aggSize) * maxBuckets;
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        maxBuckets,
        0.f,
        startingNumBuckets
    );
    grouper.initVectorized(maxVectorSize);

    int tableStart = grouper.getTableStart();

    // two keys should not cause buffer to grow
    fillKeyspace(keySpace, maxVectorSize, 2);
    AggregateResult result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(tableStart, grouper.getTableStart());

    // 3rd key should cause buffer to grow
    // buffer should grow to maximum size
    fillKeyspace(keySpace, maxVectorSize, 3);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(0, grouper.getTableStart());
  }

  @Test
  public void testGrowTwice()
  {
    final int maxVectorSize = 512;
    final int keySize = 4;
    final int aggSize = 8;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * maxVectorSize);

    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregatorAdapters.spaceNeeded()).thenReturn(aggSize);

    int startingNumBuckets = 4;
    int maxBuckets = 32;
    final int bufferSize = (keySize + aggSize) * maxBuckets;
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        maxBuckets,
        0.f,
        startingNumBuckets
    );
    grouper.initVectorized(maxVectorSize);

    int tableStart = grouper.getTableStart();

    // two keys should not cause buffer to grow
    fillKeyspace(keySpace, maxVectorSize, 2);
    AggregateResult result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(tableStart, grouper.getTableStart());

    // 3rd key should cause buffer to grow
    // buffer should grow to next size, but is not full
    fillKeyspace(keySpace, maxVectorSize, 3);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);

    // this time should be all the way
    fillKeyspace(keySpace, maxVectorSize, 6);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(0, grouper.getTableStart());
  }

  @Test
  public void testGrowThreeTimes()
  {
    final int maxVectorSize = 512;
    final int keySize = 4;
    final int aggSize = 8;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * maxVectorSize);

    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregatorAdapters.spaceNeeded()).thenReturn(aggSize);

    int startingNumBuckets = 4;
    int maxBuckets = 64;
    final int bufferSize = (keySize + aggSize) * maxBuckets;
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        maxBuckets,
        0.f,
        startingNumBuckets
    );
    grouper.initVectorized(maxVectorSize);

    int tableStart = grouper.getTableStart();

    // two keys should cause buffer to grow
    fillKeyspace(keySpace, maxVectorSize, 2);
    AggregateResult result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(tableStart, grouper.getTableStart());

    // 3rd key should cause buffer to grow
    // buffer should grow to next size, but is not full
    fillKeyspace(keySpace, maxVectorSize, 3);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);
    tableStart = grouper.getTableStart();

    // grow it again
    fillKeyspace(keySpace, maxVectorSize, 6);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);

    // this time should be all the way
    fillKeyspace(keySpace, maxVectorSize, 14);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(0, grouper.getTableStart());
  }

  @Test
  public void testGrowFourTimes()
  {
    final int maxVectorSize = 512;
    final int keySize = 4;
    final int aggSize = 8;
    final WritableMemory keySpace = WritableMemory.allocate(keySize * maxVectorSize);

    final AggregatorAdapters aggregatorAdapters = Mockito.mock(AggregatorAdapters.class);
    Mockito.when(aggregatorAdapters.spaceNeeded()).thenReturn(aggSize);

    int startingNumBuckets = 4;
    int maxBuckets = 128;
    final int bufferSize = (keySize + aggSize) * maxBuckets;
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferSize]);
    final HashVectorGrouper grouper = new HashVectorGrouper(
        Suppliers.ofInstance(buffer),
        keySize,
        aggregatorAdapters,
        maxBuckets,
        0.f,
        startingNumBuckets
    );
    grouper.initVectorized(maxVectorSize);

    int tableStart = grouper.getTableStart();

    // two keys should cause buffer to grow
    fillKeyspace(keySpace, maxVectorSize, 2);
    AggregateResult result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(tableStart, grouper.getTableStart());

    // 3rd key should cause buffer to grow
    // buffer should grow to next size, but is not full
    fillKeyspace(keySpace, maxVectorSize, 3);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);
    tableStart = grouper.getTableStart();

    // grow it again
    fillKeyspace(keySpace, maxVectorSize, 6);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);
    tableStart = grouper.getTableStart();

    // more
    fillKeyspace(keySpace, maxVectorSize, 14);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertTrue(grouper.getTableStart() > tableStart);

    // this time should be all the way
    fillKeyspace(keySpace, maxVectorSize, 25);
    result = grouper.aggregateVector(keySpace, 0, maxVectorSize);
    Assert.assertTrue(result.isOk());
    Assert.assertEquals(0, grouper.getTableStart());
  }

  private void fillKeyspace(WritableMemory keySpace, int maxVectorSize, int distinctKeys)
  {
    for (int i = 0; i < maxVectorSize; i++) {
      int bucket = i % distinctKeys;
      keySpace.putInt(((long) Integer.BYTES * i), bucket);
    }
  }
}
