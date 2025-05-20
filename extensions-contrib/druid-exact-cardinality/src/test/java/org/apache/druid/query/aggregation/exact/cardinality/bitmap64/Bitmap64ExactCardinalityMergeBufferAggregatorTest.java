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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import org.apache.druid.segment.ColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class Bitmap64ExactCardinalityMergeBufferAggregatorTest
{
  private ColumnValueSelector<Bitmap64Counter> mockSelector;
  private Bitmap64ExactCardinalityMergeBufferAggregator aggregator;
  private ByteBuffer buffer;
  private static final int BUFFER_CAPACITY = 1024;
  private static final int POSITION_1 = 0;
  private static final int POSITION_2 = 100;

  @BeforeEach
  public void setUp()
  {
    mockSelector = EasyMock.createMock(ColumnValueSelector.class);
    aggregator = new Bitmap64ExactCardinalityMergeBufferAggregator(mockSelector);
    buffer = ByteBuffer.allocate(BUFFER_CAPACITY);
  }

  @Test
  public void testInit()
  {
    aggregator.init(buffer, POSITION_1);
    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(0, counter.getCardinality());
  }

  @Test
  public void testAggregateWithNonNullCounter()
  {
    aggregator.init(buffer, POSITION_1);
    RoaringBitmap64Counter inputCounter = new RoaringBitmap64Counter();
    inputCounter.add(123L);

    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter resultCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, resultCounter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateWithNullCounterFromSelector()
  {
    aggregator.init(buffer, POSITION_1);
    Bitmap64Counter initialCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    long initialCardinality = initialCounter.getCardinality();

    EasyMock.expect(mockSelector.getObject()).andReturn(null).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1); // Should not throw NPE, and not change the counter

    Bitmap64Counter resultCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(
        initialCardinality,
        resultCounter.getCardinality(),
        "Aggregating null from selector should not change cardinality"
    );

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateMultipleCounters()
  {
    aggregator.init(buffer, POSITION_1);
    RoaringBitmap64Counter inputCounter1 = new RoaringBitmap64Counter();
    inputCounter1.add(10L);
    RoaringBitmap64Counter inputCounter2 = new RoaringBitmap64Counter();
    inputCounter2.add(20L);
    inputCounter2.add(10L); // duplicate

    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter1).once();
    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter2).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter resultCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(2, resultCounter.getCardinality()); // 10, 20

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateAtDifferentPositions()
  {
    aggregator.init(buffer, POSITION_1);
    aggregator.init(buffer, POSITION_2);

    RoaringBitmap64Counter counterForPos1 = new RoaringBitmap64Counter();
    counterForPos1.add(1L);
    RoaringBitmap64Counter counterForPos2 = new RoaringBitmap64Counter();
    counterForPos2.add(2L);

    EasyMock.expect(mockSelector.getObject()).andReturn(counterForPos1).once(); // For POSITION_1
    EasyMock.expect(mockSelector.getObject()).andReturn(counterForPos2).once(); // For POSITION_2
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_2);

    Bitmap64Counter result1 = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, result1.getCardinality());

    Bitmap64Counter result2 = (Bitmap64Counter) aggregator.get(buffer, POSITION_2);
    Assertions.assertEquals(1, result2.getCardinality());

    Assertions.assertNotSame(result1, result2);
    EasyMock.verify(mockSelector);
  }

  @Test
  public void testCloseClearsCache()
  {
    aggregator.init(buffer, POSITION_1);
    RoaringBitmap64Counter inputCounter = new RoaringBitmap64Counter();
    inputCounter.add(1L);
    EasyMock.expect(mockSelector.getObject()).andReturn(inputCounter).once();
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);
    EasyMock.verify(mockSelector);

    Bitmap64Counter counterBeforeClose = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertNotNull(counterBeforeClose);

    aggregator.close();

    // After close, accessing the same buffer and position might throw NPE or return null
    // depending on how IdentityHashMap and Int2ObjectMap behave after clearing.
    // The current implementation of get() would lead to NPE if buffer is not in counterCache.
    Assertions.assertThrows(
        NullPointerException.class,
        () -> aggregator.get(buffer, POSITION_1),
        "Accessing counter from cleared cache should fail or return null"
    );
  }

  @Test
  public void testRelocateSameBuffer()
  {
    aggregator.init(buffer, POSITION_1);
    RoaringBitmap64Counter initialCounterVal = new RoaringBitmap64Counter();
    initialCounterVal.add(123L);

    EasyMock.expect(mockSelector.getObject()).andReturn(initialCounterVal).once();
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);
    EasyMock.verify(mockSelector);

    Bitmap64Counter originalCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, originalCounter.getCardinality());

    aggregator.relocate(POSITION_1, POSITION_2, buffer, buffer);

    Bitmap64Counter newCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_2);
    Assertions.assertNotNull(newCounter);
    Assertions.assertEquals(1, newCounter.getCardinality());
    Assertions.assertSame(originalCounter, newCounter);
  }

  @Test
  public void testRelocateDifferentBuffers()
  {
    aggregator.init(buffer, POSITION_1);
    RoaringBitmap64Counter initialCounterVal = new RoaringBitmap64Counter();
    initialCounterVal.add(456L);

    EasyMock.expect(mockSelector.getObject()).andReturn(initialCounterVal).once();
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);
    EasyMock.verify(mockSelector);

    Bitmap64Counter originalCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, originalCounter.getCardinality());

    ByteBuffer newBuffer = ByteBuffer.allocate(BUFFER_CAPACITY);
    aggregator.relocate(POSITION_1, POSITION_1, buffer, newBuffer);

    Bitmap64Counter newCounter = (Bitmap64Counter) aggregator.get(newBuffer, POSITION_1);
    Assertions.assertNotNull(newCounter);
    Assertions.assertEquals(1, newCounter.getCardinality());
    Assertions.assertSame(originalCounter, newCounter);
  }

  @Test
  public void testUnsupportedGetOperations()
  {
    aggregator.init(buffer, POSITION_1); // Ensure the entry exists in cache to avoid NPE on get()
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getLong(buffer, POSITION_1));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getDouble(buffer, POSITION_1));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getFloat(buffer, POSITION_1));
  }
} 