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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SpectatorHistogramAggregateHelperTest extends InitializedNullHandlingTest
{
  private static final int POSITION = 0;
  private static final int POSITION_2 = 1;

  private SpectatorHistogramAggregateHelper helper;
  private ByteBuffer buffer;

  @Before
  public void setUp()
  {
    helper = new SpectatorHistogramAggregateHelper();
    buffer = ByteBuffer.allocate(1024);
  }

  @After
  public void tearDown()
  {
    helper.close();
  }

  @Test
  public void testInitCreatesEmptyHistogram()
  {
    helper.init(buffer, POSITION);
    SpectatorHistogram result = helper.get(buffer, POSITION);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testMergeWithSpectatorHistogramObject()
  {
    SpectatorHistogram other = new SpectatorHistogram();
    other.add(PercentileBuckets.indexOf(100), 5L);
    other.add(PercentileBuckets.indexOf(200), 3L);

    helper.init(buffer, POSITION);
    SpectatorHistogram histogram = helper.get(buffer, POSITION);
    helper.merge(histogram, other);

    SpectatorHistogram result = helper.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(other, result);
  }

  @Test
  public void testMergeWithLongValues()
  {
    helper.init(buffer, POSITION);
    SpectatorHistogram histogram = helper.get(buffer, POSITION);
    helper.merge(histogram, 100L);
    helper.merge(histogram, 200L);
    helper.merge(histogram, 100L);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);
    expected.insert(200L);
    expected.insert(100L);

    SpectatorHistogram result = helper.get(buffer, POSITION);
    Assert.assertNotNull(result);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMergeWithSpectatorHistogram()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(100), 5L);

    SpectatorHistogram other = new SpectatorHistogram();
    other.add(PercentileBuckets.indexOf(100), 3L);
    other.add(PercentileBuckets.indexOf(200), 2L);

    helper.merge(histogram, other);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(100), 8L);
    expected.add(PercentileBuckets.indexOf(200), 2L);

    Assert.assertEquals(expected, histogram);
  }

  @Test
  public void testMergeWithNumber()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();

    helper.merge(histogram, 100);
    helper.merge(histogram, 200L);
    helper.merge(histogram, 300.0);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100);
    expected.insert(200L);
    expected.insert(300.0);

    Assert.assertEquals(expected, histogram);
  }

  @Test
  public void testMergeWithInvalidType()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();

    Assert.assertThrows(IAE.class, () -> helper.merge(histogram, "invalid"));
  }

  @Test
  public void testMergeLongValue()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();

    helper.merge(histogram, 100L);
    helper.merge(histogram, 100L);
    helper.merge(histogram, 200L);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.insert(100L);
    expected.insert(100L);
    expected.insert(200L);

    Assert.assertEquals(expected, histogram);
  }

  @Test
  public void testGetFromCache()
  {
    helper.init(buffer, POSITION);
    SpectatorHistogram histogram = helper.get(buffer, POSITION);

    SpectatorHistogram other = new SpectatorHistogram();
    other.add(PercentileBuckets.indexOf(100), 5L);
    helper.merge(histogram, other);

    // Multiple gets should return the same cached histogram
    SpectatorHistogram result1 = helper.get(buffer, POSITION);
    SpectatorHistogram result2 = helper.get(buffer, POSITION);

    Assert.assertSame(result1, result2);
  }

  @Test
  public void testGetBufferMap()
  {
    helper.init(buffer, POSITION);
    helper.init(buffer, POSITION_2);

    SpectatorHistogram hist1 = helper.get(buffer, POSITION);
    SpectatorHistogram hist2 = helper.get(buffer, POSITION_2);

    hist1.add(PercentileBuckets.indexOf(100), 1L);
    hist2.add(PercentileBuckets.indexOf(200), 2L);

    Int2ObjectMap<SpectatorHistogram> map = helper.get(buffer);
    Assert.assertNotNull(map);
    Assert.assertEquals(2, map.size());
    Assert.assertSame(hist1, map.get(POSITION));
    Assert.assertSame(hist2, map.get(POSITION_2));
  }

  @Test
  public void testMultiplePositions()
  {
    SpectatorHistogram other1 = new SpectatorHistogram();
    other1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram other2 = new SpectatorHistogram();
    other2.add(PercentileBuckets.indexOf(200), 2L);

    helper.init(buffer, POSITION);
    helper.init(buffer, POSITION_2);

    SpectatorHistogram hist1 = helper.get(buffer, POSITION);
    SpectatorHistogram hist2 = helper.get(buffer, POSITION_2);

    helper.merge(hist1, other1);
    helper.merge(hist2, other2);

    SpectatorHistogram result1 = helper.get(buffer, POSITION);
    SpectatorHistogram result2 = helper.get(buffer, POSITION_2);

    Assert.assertEquals(other1, result1);
    Assert.assertEquals(other2, result2);
  }

  @Test
  public void testRelocate()
  {
    SpectatorHistogram other = new SpectatorHistogram();
    other.add(PercentileBuckets.indexOf(100), 5L);

    ByteBuffer oldBuffer = ByteBuffer.allocate(1024);
    ByteBuffer newBuffer = ByteBuffer.allocate(1024);
    int oldPosition = 0;
    int newPosition = 1;

    helper.init(oldBuffer, oldPosition);
    SpectatorHistogram histogram = helper.get(oldBuffer, oldPosition);
    helper.merge(histogram, other);

    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);

    SpectatorHistogram result = helper.get(newBuffer, newPosition);
    Assert.assertEquals(other, result);
  }

  @Test
  public void testRelocateRemovesOldEntry()
  {
    SpectatorHistogram other = new SpectatorHistogram();
    other.add(PercentileBuckets.indexOf(100), 5L);

    ByteBuffer oldBuffer = ByteBuffer.allocate(1024);
    ByteBuffer newBuffer = ByteBuffer.allocate(1024);
    int oldPosition = 0;
    int newPosition = 1;

    helper.init(oldBuffer, oldPosition);
    SpectatorHistogram histogram = helper.get(oldBuffer, oldPosition);
    helper.merge(histogram, other);

    helper.relocate(oldPosition, newPosition, oldBuffer, newBuffer);

    // Old position should no longer have the histogram
    Assert.assertNull(helper.get(oldBuffer, oldPosition));
  }

  @Test
  public void testClose()
  {
    helper.init(buffer, POSITION);
    SpectatorHistogram histogram = helper.get(buffer, POSITION);
    helper.merge(histogram, 100L);

    helper.close();

    // After close, the cache should be cleared
    Assert.assertNull(helper.get(buffer, POSITION));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetFloatThrowsUnsupportedOperationException()
  {
    helper.init(buffer, POSITION);
    helper.getFloat(buffer, POSITION);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLongThrowsUnsupportedOperationException()
  {
    helper.init(buffer, POSITION);
    helper.getLong(buffer, POSITION);
  }

  @Test
  public void testMergeMultipleHistograms()
  {
    SpectatorHistogram other1 = new SpectatorHistogram();
    other1.add(PercentileBuckets.indexOf(100), 1L);

    SpectatorHistogram other2 = new SpectatorHistogram();
    other2.add(PercentileBuckets.indexOf(100), 2L);
    other2.add(PercentileBuckets.indexOf(200), 1L);

    SpectatorHistogram other3 = new SpectatorHistogram();
    other3.add(PercentileBuckets.indexOf(300), 3L);

    helper.init(buffer, POSITION);
    SpectatorHistogram histogram = helper.get(buffer, POSITION);

    helper.merge(histogram, other1);
    helper.merge(histogram, other2);
    helper.merge(histogram, other3);

    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(100), 3L);
    expected.add(PercentileBuckets.indexOf(200), 1L);
    expected.add(PercentileBuckets.indexOf(300), 3L);

    SpectatorHistogram result = helper.get(buffer, POSITION);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetNonExistentBuffer()
  {
    ByteBuffer otherBuffer = ByteBuffer.allocate(1024);
    Assert.assertNull(helper.get(otherBuffer, POSITION));
    Assert.assertNull(helper.get(otherBuffer));
  }
}
