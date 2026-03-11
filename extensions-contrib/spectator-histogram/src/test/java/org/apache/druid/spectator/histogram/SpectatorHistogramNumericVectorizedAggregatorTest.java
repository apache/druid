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

package org.apache.druid.spectator.histogram;

import com.netflix.spectator.api.histogram.PercentileBuckets;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class SpectatorHistogramNumericVectorizedAggregatorTest extends InitializedNullHandlingTest
{
  private static final int POSITION = 0;
  private static final int POSITION_2 = 1;

  @Mock
  private VectorValueSelector selector;

  private SpectatorHistogramNumericVectorizedAggregator aggregator;
  private ByteBuffer buffer;

  @Before
  public void setUp()
  {
    aggregator = new SpectatorHistogramNumericVectorizedAggregator(selector);
    buffer = ByteBuffer.allocate(1024);
  }

  @Test
  public void testInitCreatesEmptyHistogram()
  {
    aggregator.init(buffer, POSITION);
    Object result = aggregator.get(buffer, POSITION);

    // Empty histogram returns null
    Assert.assertNull(result);
  }

  @Test
  public void testAggregateWithLongValues()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);
    expected.insert(200L);
    expected.insert(300L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithNullValues()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    boolean[] nullVector = new boolean[]{false, true, false};

    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(nullVector).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);
    expected.insert(300L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithAllNulls()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    boolean[] nullVector = new boolean[]{true, true, true};

    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(nullVector).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    // Should still be empty/null
    Assert.assertNull(aggregator.get(buffer, POSITION));
  }

  @Test
  public void testAggregateWithPositions()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // Aggregate row 0 and 2 into POSITION, row 1 into POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2, POSITION};
    int[] rows = new int[]{0, 1, 2};
    aggregator.aggregate(buffer, 3, positions, rows, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.insert(100L);
    expected1.insert(300L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.insert(200L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }

  @Test
  public void testAggregateWithPositionsNoRows()
  {
    long[] vector = new long[]{100L, 200L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // When rows is null, use index directly
    int[] positions = new int[]{POSITION, POSITION_2};
    aggregator.aggregate(buffer, 2, positions, null, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.insert(100L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.insert(200L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }

  @Test
  public void testAggregateWithPositionOffset()
  {
    long[] vector = new long[]{100L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    int actualPosition = 5;
    aggregator.init(buffer, actualPosition);

    // positions[0] + positionOffset should equal actualPosition
    int[] positions = new int[]{3};
    int positionOffset = 2;
    aggregator.aggregate(buffer, 1, positions, null, positionOffset);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, actualPosition);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithPositionsAndNulls()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    boolean[] nullVector = new boolean[]{false, true, false};

    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(nullVector).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // Aggregate row 0, 1, 2 into different positions
    // Row 1 is null, so it should be skipped
    int[] positions = new int[]{POSITION, POSITION_2, POSITION};
    aggregator.aggregate(buffer, 3, positions, null, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.insert(100L);
    expected1.insert(300L);

    // Position 2 should be empty because row 1 was null
    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertNull(result2);
  }

  @Test
  public void testAggregatePartialRange()
  {
    long[] vector = new long[]{100L, 200L, 300L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    // Only aggregate rows 1 to 2 (exclusive of 2), so just 200L
    aggregator.aggregate(buffer, POSITION, 1, 2);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(200L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateMultipleBatches()
  {
    long[] vector1 = new long[]{100L, 200L};
    long[] vector2 = new long[]{300L, 400L};

    aggregator.init(buffer, POSITION);

    // First batch
    Mockito.doReturn(vector1).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();
    aggregator.aggregate(buffer, POSITION, 0, 2);

    // Second batch
    Mockito.doReturn(vector2).when(selector).getLongVector();
    aggregator.aggregate(buffer, POSITION, 0, 2);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);
    expected.insert(200L);
    expected.insert(300L);
    expected.insert(400L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testClose()
  {
    long[] vector = new long[]{100L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 1);

    // Verify aggregation worked
    Assert.assertNotNull(aggregator.get(buffer, POSITION));

    // Close should clear the cache
    aggregator.close();

    // After close, get should return null
    Assert.assertNull(aggregator.get(buffer, POSITION));
  }

  @Test
  public void testValuesGoToCorrectBuckets()
  {
    // Test that values are correctly bucketed using PercentileBuckets
    long[] vector = new long[]{10L, 100L, 1000L, 10000L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 4);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);

    // Verify each value went to its correct bucket
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10L), 1L);
    expected.add(PercentileBuckets.indexOf(100L), 1L);
    expected.add(PercentileBuckets.indexOf(1000L), 1L);
    expected.add(PercentileBuckets.indexOf(10000L), 1L);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testRelocate()
  {
    long[] vector = new long[]{100L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    ByteBuffer oldBuffer = ByteBuffer.allocate(1024);
    ByteBuffer newBuffer = ByteBuffer.allocate(1024);
    int oldPosition = 0;
    int newPosition = 1;

    aggregator.init(oldBuffer, oldPosition);
    aggregator.aggregate(oldBuffer, oldPosition, 0, 1);

    aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(newBuffer, newPosition);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithNonSequentialRowsAndNulls()
  {
    long[] vector = new long[]{100L, 200L, 300L, 400L, 500L};
    // nullVector[1]=true means vector[1] is null
    // nullVector[4]=false means vector[4] is not null
    boolean[] nullVector = new boolean[]{false, true, false, false, false};

    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(nullVector).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // rows[0]=4 -> vector[4]=500, nullVector[4]=false -> aggregates to POSITION
    // rows[1]=1 -> vector[1]=200, nullVector[1]=true -> skipped for POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2};
    int[] rows = new int[]{4, 1};
    aggregator.aggregate(buffer, 2, positions, rows, 0);

    // POSITION should have value from vector[4] = 500
    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.insert(500L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    // POSITION_2 should be null/empty because nullVector[rows[1]]=nullVector[1]=true
    Assert.assertNull(result2);
  }

  @Test
  public void testAggregateWithNonSequentialRows()
  {
    long[] vector = new long[]{100L, 200L, 300L, 400L, 500L};
    Mockito.doReturn(vector).when(selector).getLongVector();
    Mockito.doReturn(null).when(selector).getNullVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // rows[0]=4 -> vector[4]=500 -> POSITION
    // rows[1]=1 -> vector[1]=200 -> POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2};
    int[] rows = new int[]{4, 1};
    aggregator.aggregate(buffer, 2, positions, rows, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.insert(500L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.insert(200L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }
}
