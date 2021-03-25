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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IndexMergerLongestSharedDimOrderTest
{
  @Mock
  Supplier<ColumnHolder> mockSupplier;

  @Mock
  ColumnHolder mockColumnHolder;

  @Mock
  SmooshedFileMapper mockSmooshedFileMapper;

  @Mock
  BitmapFactory mockBitmapFactory;

  @Before
  public void setUp()
  {
    when(mockSupplier.get()).thenReturn(mockColumnHolder);
    // This value does not matter
    when(mockColumnHolder.getLength()).thenReturn(1);
  }

  @Test
  public void testGetLongestSharedDimOrderWithNullDimensionSpecAndEmptyIndex()
  {
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(), null);
    Assert.assertNull(actual);
  }

  @Test
  public void testGetLongestSharedDimOrderWithNullDimensionSpecAndValidOrdering()
  {
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("b", "c"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), actual);

    //  Valid ordering as although second index has gap, it is still same ordering
    index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    index2 = makeIndexWithDimensionList(ImmutableList.of("a", "c"));
    actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), null);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), actual);
  }

  @Test
  public void testGetLongestSharedDimOrderWithNullDimensionSpecAndNoValidOrdering()
  {
    // No valid ordering as no index as all three dimensions
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("b", "c"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), null);
    Assert.assertNull(actual);

    //  No valid ordering as ordering is not the same in all indexes
    index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    index2 = makeIndexWithDimensionList(ImmutableList.of("c", "b"));
    actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), null);
    Assert.assertNull(actual);
  }


  @Test
  public void testGetLongestSharedDimOrderWithSchemalessDimensionSpecAndNoValidOrdering()
  {
    DimensionsSpec empty = new DimensionsSpec(ImmutableList.of());
    // No valid ordering as no index as all three dimensions
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("b", "c"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), empty);
    Assert.assertNull(actual);

    //  No valid ordering as ordering is not the same in all indexes
    index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    index2 = makeIndexWithDimensionList(ImmutableList.of("c", "b"));
    actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), empty);
    Assert.assertNull(actual);
  }

  @Test
  public void testGetLongestSharedDimOrderWithValidSchemaDimensionSpecAndNoValidOrdering()
  {
    DimensionsSpec valid = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("a", "b", "c")));
    // No valid ordering as no index has all three dimensions
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("b", "c"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), valid);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), actual);
  }

  @Test
  public void testGetLongestSharedDimOrderWithInvalidSchemaDimensionSpecAndNoValidOrdering()
  {
    DimensionsSpec valid = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("a", "b", "c")));
    //  No valid ordering as ordering is not the same in all indexes
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("c", "b"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), valid);
    // Since ordering of index2 is not the same as the ordering of the schema in DimensionSpec
    Assert.assertNull(actual);
  }

  @Test
  public void testGetLongestSharedDimOrderWithValidSchemaDimensionSpecAndInvalidOrdering()
  {
    DimensionsSpec valid = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("a", "b", "c")));
    //  No valid ordering as ordering is not the same in all indexes
    QueryableIndexIndexableAdapter index1 = makeIndexWithDimensionList(ImmutableList.of("a", "b", "c"));
    QueryableIndexIndexableAdapter index2 = makeIndexWithDimensionList(ImmutableList.of("c", "b", "e"));
    List<String> actual = IndexMerger.getLongestSharedDimOrder(ImmutableList.of(index1, index2), valid);
    // Since index2 has dimension that is not in the schema in DimensionSpec. This should not be possible.
    Assert.assertNull(actual);
  }

  private QueryableIndexIndexableAdapter makeIndexWithDimensionList(List<String> dimensions)
  {
    return new QueryableIndexIndexableAdapter(
        new SimpleQueryableIndex(
            new Interval("2012-01-01/2012-01-02", ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"))),
            new ListIndexed<>(dimensions),
            mockBitmapFactory,
            ImmutableMap.of(ColumnHolder.TIME_COLUMN_NAME, mockSupplier),
            mockSmooshedFileMapper,
            null,
            true
        )
    );
  }
}


