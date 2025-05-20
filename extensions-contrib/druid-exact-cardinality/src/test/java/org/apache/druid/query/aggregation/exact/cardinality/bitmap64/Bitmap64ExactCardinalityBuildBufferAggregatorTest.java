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

import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class Bitmap64ExactCardinalityBuildBufferAggregatorTest
{
  private BaseLongColumnValueSelector mockSelector;
  private Bitmap64ExactCardinalityBuildBufferAggregator aggregator;
  private ByteBuffer buffer;
  private static final int BUFFER_CAPACITY = 1024;
  private static final int POSITION_1 = 0;
  private static final int POSITION_2 = 100; // Another distinct position

  @BeforeEach
  public void setUp()
  {
    mockSelector = EasyMock.createMock(BaseLongColumnValueSelector.class);
    aggregator = new Bitmap64ExactCardinalityBuildBufferAggregator(mockSelector);
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
  public void testAggregateSingleValue()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(123L).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateCreatesCollectorIfNotExists()
  {
    // No init call, aggregate should create it
    EasyMock.expect(mockSelector.getLong()).andReturn(456L).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(1, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateMultipleDistinctValues()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(20L).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(2, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateWithDuplicates()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(20L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once(); // Duplicate
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_1);

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(2, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateAtDifferentPositions()
  {
    aggregator.init(buffer, POSITION_1);
    aggregator.init(buffer, POSITION_2);

    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once(); // For POSITION_1
    EasyMock.expect(mockSelector.getLong()).andReturn(20L).once(); // For POSITION_2
    EasyMock.replay(mockSelector);

    aggregator.aggregate(buffer, POSITION_1);
    aggregator.aggregate(buffer, POSITION_2);

    Bitmap64Counter counter1 = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, counter1.getCardinality());

    Bitmap64Counter counter2 = (Bitmap64Counter) aggregator.get(buffer, POSITION_2);
    Assertions.assertEquals(1, counter2.getCardinality());

    Assertions.assertNotSame(counter1, counter2);

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testGetWithoutInitOrAggregateReturnsNewCounter()
  {
    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(0, counter.getCardinality(), "Getting for a new position should return an empty counter");
  }

  @Test
  public void testCloseIsNoOp()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once();
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);

    aggregator.close(); // Should be a no-op

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertNotNull(counter, "Counter should still exist after close");
    Assertions.assertEquals(1, counter.getCardinality());
    EasyMock.verify(mockSelector);
  }

  @Test
  public void testRelocateSameBuffer()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(123L).times(1);
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);
    EasyMock.verify(mockSelector);

    Bitmap64Counter originalCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, originalCounter.getCardinality());

    aggregator.relocate(POSITION_1, POSITION_2, buffer, buffer);

    Bitmap64Counter newCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_2);
    Assertions.assertNotNull(newCounter);
    Assertions.assertEquals(1, newCounter.getCardinality());
    Assertions.assertSame(originalCounter, newCounter, "Relocate in same buffer should move the same counter instance");
  }

  @Test
  public void testRelocateDifferentBuffers()
  {
    aggregator.init(buffer, POSITION_1);
    EasyMock.expect(mockSelector.getLong()).andReturn(456L).times(1);
    EasyMock.replay(mockSelector);
    aggregator.aggregate(buffer, POSITION_1);
    EasyMock.verify(mockSelector);

    Bitmap64Counter originalCounter = (Bitmap64Counter) aggregator.get(buffer, POSITION_1);
    Assertions.assertEquals(1, originalCounter.getCardinality());

    ByteBuffer newBuffer = ByteBuffer.allocate(BUFFER_CAPACITY);
    aggregator.relocate(POSITION_1, POSITION_1, buffer, newBuffer); // new buffer, same position offset

    Bitmap64Counter newCounter = (Bitmap64Counter) aggregator.get(newBuffer, POSITION_1);
    Assertions.assertNotNull(newCounter);
    Assertions.assertEquals(1, newCounter.getCardinality());
    Assertions.assertSame(
        originalCounter,
        newCounter,
        "Relocate to different buffer should move the same counter instance"
    );
  }

  @Test
  public void testUnsupportedGetOperations()
  {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getLong(buffer, POSITION_1));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getDouble(buffer, POSITION_1));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getFloat(buffer, POSITION_1));
  }
}
