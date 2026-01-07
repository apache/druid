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
import org.apache.druid.segment.vector.VectorObjectSelector;
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
public class SpectatorHistogramVectorizedAggregatorTest extends InitializedNullHandlingTest
{
  private static final int POSITION = 0;
  private static final int POSITION_2 = 1;

  @Mock
  private VectorObjectSelector selector;

  private SpectatorHistogramVectorizedAggregator aggregator;
  private ByteBuffer buffer;

  @Before
  public void setUp()
  {
    aggregator = new SpectatorHistogramVectorizedAggregator(selector);
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
  public void testAggregateWithSpectatorHistograms()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 5L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(100), 3L);
    hist2.add(PercentileBuckets.indexOf(200), 2L);

    Object[] vector = new Object[]{hist1, hist2};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 2);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(100), 8L);
    expected.add(PercentileBuckets.indexOf(200), 2L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithNullValues()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 5L);

    Object[] vector = new Object[]{null, hist1, null};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(hist1, result);
  }

  @Test
  public void testAggregateWithAllNulls()
  {
    Object[] vector = new Object[]{null, null, null};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    // Should still be empty/null
    Assert.assertNull(aggregator.get(buffer, POSITION));
  }

  @Test
  public void testAggregateWithNumberValues()
  {
    // The vectorized aggregator for objects can also handle Number objects
    Object[] vector = new Object[]{100L, 200L, 300L};
    Mockito.doReturn(vector).when(selector).getObjectVector();

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
  public void testAggregateWithPositions()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram hist3 = new SpectatorHistogram();
    hist3.add(PercentileBuckets.indexOf(300), 1L);

    Object[] vector = new Object[]{hist1, hist2, hist3};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // Aggregate row 0 and 2 into POSITION, row 1 into POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2, POSITION};
    int[] rows = new int[]{0, 1, 2};
    aggregator.aggregate(buffer, 3, positions, rows, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.add(PercentileBuckets.indexOf(100), 1L);
    expected1.add(PercentileBuckets.indexOf(300), 1L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }

  @Test
  public void testAggregateWithPositionsNoRows()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(200), 1L);

    Object[] vector = new Object[]{hist1, hist2};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // When rows is null, use index directly
    int[] positions = new int[]{POSITION, POSITION_2};
    aggregator.aggregate(buffer, 2, positions, null, 0);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(hist1, result1);
    Assert.assertEquals(hist2, result2);
  }

  @Test
  public void testAggregateWithPositionOffset()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    Object[] vector = new Object[]{hist1};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    int actualPosition = 5;
    aggregator.init(buffer, actualPosition);

    // positions[0] + positionOffset should equal actualPosition
    int[] positions = new int[]{3};
    int positionOffset = 2;
    aggregator.aggregate(buffer, 1, positions, null, positionOffset);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, actualPosition);
    Assert.assertEquals(hist1, result);
  }

  @Test
  public void testRelocate()
  {
    SpectatorHistogram hist = new SpectatorHistogram();
    hist.add(PercentileBuckets.indexOf(100), 5L);

    Object[] vector = new Object[]{hist};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    ByteBuffer oldBuffer = ByteBuffer.allocate(1024);
    ByteBuffer newBuffer = ByteBuffer.allocate(1024);
    int oldPosition = 0;
    int newPosition = 1;

    aggregator.init(oldBuffer, oldPosition);
    aggregator.aggregate(oldBuffer, oldPosition, 0, 1);

    aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(newBuffer, newPosition);
    Assert.assertEquals(hist, result);
  }

  @Test
  public void testAggregatePartialRange()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram hist3 = new SpectatorHistogram();
    hist3.add(PercentileBuckets.indexOf(300), 1L);

    Object[] vector = new Object[]{hist1, hist2, hist3};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    // Only aggregate rows 1 to 2 (exclusive of 2), so just hist2
    aggregator.aggregate(buffer, POSITION, 1, 2);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertEquals(hist2, result);
  }

  @Test
  public void testClose()
  {
    SpectatorHistogram hist = new SpectatorHistogram();
    hist.add(PercentileBuckets.indexOf(100), 5L);

    Object[] vector = new Object[]{hist};
    Mockito.doReturn(vector).when(selector).getObjectVector();

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
  public void testAggregateMultipleBatches()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(200), 2L);

    aggregator.init(buffer, POSITION);

    // First batch
    Object[] vector1 = new Object[]{hist1};
    Mockito.doReturn(vector1).when(selector).getObjectVector();
    aggregator.aggregate(buffer, POSITION, 0, 1);

    // Second batch
    Object[] vector2 = new Object[]{hist2};
    Mockito.doReturn(vector2).when(selector).getObjectVector();
    aggregator.aggregate(buffer, POSITION, 0, 1);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(100), 1L);
    expected.add(PercentileBuckets.indexOf(200), 2L);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithPositionsMultipleToSamePosition()
  {
    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist2 = new SpectatorHistogram();
    hist2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram hist3 = new SpectatorHistogram();
    hist3.add(PercentileBuckets.indexOf(300), 1L);

    // Vector with histograms
    Object[] vector = new Object[]{hist1, hist2, hist3};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // Aggregate rows 0 and 2 to POSITION, row 1 to POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2, POSITION};
    aggregator.aggregate(buffer, 3, positions, null, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.add(PercentileBuckets.indexOf(100), 1L);
    expected1.add(PercentileBuckets.indexOf(300), 1L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }

  @Test
  public void testAggregateWithMixedTypesInVector()
  {
    // Mix of SpectatorHistogram and Number objects
    SpectatorHistogram hist = new SpectatorHistogram();
    hist.add(PercentileBuckets.indexOf(100), 1L);

    Object[] vector = new Object[]{hist, 200L, 300};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.aggregate(buffer, POSITION, 0, 3);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(100), 1L);
    expected.insert(200L);
    expected.insert(300);

    SpectatorHistogram result = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testAggregateWithNonSequentialRowsAndNulls()
  {
    // For object vectors, null checking is done on the object itself (vector[rowIndex] != null),
    // so nulls in the vector at the positions pointed to by rows[] are correctly skipped.
    SpectatorHistogram hist0 = new SpectatorHistogram();
    hist0.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist4 = new SpectatorHistogram();
    hist4.add(PercentileBuckets.indexOf(500), 1L);

    // vector[1] is null - this will be skipped when rows points to it
    Object[] vector = new Object[]{hist0, null, null, null, hist4};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // rows[0]=4 -> vector[4]=hist4, not null -> aggregates to POSITION
    // rows[1]=1 -> vector[1]=null -> skipped for POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2};
    int[] rows = new int[]{4, 1};
    aggregator.aggregate(buffer, 2, positions, rows, 0);

    // POSITION should have value from vector[4] = hist4
    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.add(PercentileBuckets.indexOf(500), 1L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    // POSITION_2 should be null/empty because vector[rows[1]]=vector[1]=null
    Assert.assertNull(result2);
  }

  @Test
  public void testAggregateWithNonSequentialRows()
  {
    SpectatorHistogram hist0 = new SpectatorHistogram();
    hist0.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram hist1 = new SpectatorHistogram();
    hist1.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram hist4 = new SpectatorHistogram();
    hist4.add(PercentileBuckets.indexOf(500), 1L);

    Object[] vector = new Object[]{hist0, hist1, null, null, hist4};
    Mockito.doReturn(vector).when(selector).getObjectVector();

    aggregator.init(buffer, POSITION);
    aggregator.init(buffer, POSITION_2);

    // rows[0]=4 -> vector[4]=hist4 -> POSITION
    // rows[1]=1 -> vector[1]=hist1 -> POSITION_2
    int[] positions = new int[]{POSITION, POSITION_2};
    int[] rows = new int[]{4, 1};
    aggregator.aggregate(buffer, 2, positions, rows, 0);

    SpectatorHistogram expected1 = new SpectatorHistogram();
    expected1.add(PercentileBuckets.indexOf(500), 1L);

    SpectatorHistogram expected2 = new SpectatorHistogram();
    expected2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram result1 = (SpectatorHistogram) aggregator.get(buffer, POSITION);
    SpectatorHistogram result2 = (SpectatorHistogram) aggregator.get(buffer, POSITION_2);

    Assert.assertEquals(expected1, result1);
    Assert.assertEquals(expected2, result2);
  }
}
