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

public class Bitmap64ExactCardinalityBuildAggregatorTest
{
  private BaseLongColumnValueSelector mockSelector;
  private Bitmap64ExactCardinalityBuildAggregator aggregator;

  @BeforeEach
  public void setUp()
  {
    mockSelector = EasyMock.createMock(BaseLongColumnValueSelector.class);
    aggregator = new Bitmap64ExactCardinalityBuildAggregator(mockSelector);
  }

  @Test
  public void testAggregateSingleValue()
  {
    EasyMock.expect(mockSelector.getLong()).andReturn(123L).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate();

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get();
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(1, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateMultipleDistinctValues()
  {
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(20L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(30L).once();
    EasyMock.replay(mockSelector);

    aggregator.aggregate();
    aggregator.aggregate();
    aggregator.aggregate();

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get();
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(3, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testAggregateMultipleValuesWithDuplicates()
  {
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(20L).once();
    EasyMock.expect(mockSelector.getLong()).andReturn(10L).once(); // Duplicate
    EasyMock.replay(mockSelector);

    aggregator.aggregate();
    aggregator.aggregate();
    aggregator.aggregate();

    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get();
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(2, counter.getCardinality());

    EasyMock.verify(mockSelector);
  }

  @Test
  public void testGetInitialState()
  {
    Bitmap64Counter counter = (Bitmap64Counter) aggregator.get();
    Assertions.assertNotNull(counter);
    Assertions.assertEquals(0, counter.getCardinality());
  }

  @Test
  public void testClose()
  {
    aggregator.close();
    Assertions.assertNull(aggregator.get(), "Bitmap should be null after close");
  }

  @Test
  public void testUnsupportedGetOperations()
  {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getFloat());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> aggregator.getLong());
  }
}
