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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GroupByQueryEngineTest
{
  private static final String DIM = "d0";
  ColumnSelectorFactory factory;

  @BeforeEach
  public void setUp()
  {
    factory = EasyMock.createMock(ColumnSelectorFactory.class);
  }

  @Test
  public void testCanPushDownLimitForSegmentStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(true)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesSorted(true)
                                                                  .setDictionaryValuesUnique(true);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assertions.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForIncrementalStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(false)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(true);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assertions.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForExpressionStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(false)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assertions.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForJoinStringSelector()
  {
    ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                  .setHasBitmapIndexes(false)
                                                                  .setHasMultipleValues(false)
                                                                  .setDictionaryEncoded(true)
                                                                  .setDictionaryValuesSorted(false)
                                                                  .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assertions.assertFalse(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForNumericSelector()
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.LONG)
                                                                      .setHasBitmapIndexes(false)
                                                                      .setHasMultipleValues(false)
                                                                      .setDictionaryEncoded(false)
                                                                      .setDictionaryValuesSorted(false)
                                                                      .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).anyTimes();
    EasyMock.replay(factory);
    Assertions.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    capabilities.setType(ColumnType.DOUBLE);
    Assertions.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    capabilities.setType(ColumnType.FLOAT);
    Assertions.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testCanPushDownLimitForComplexSelector()
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(new ColumnType(ValueType.COMPLEX, "foo", null))
                                                                      .setHasBitmapIndexes(false)
                                                                      .setHasMultipleValues(false)
                                                                      .setDictionaryEncoded(false)
                                                                      .setDictionaryValuesSorted(false)
                                                                      .setDictionaryValuesUnique(false);
    EasyMock.expect(factory.getColumnCapabilities(DIM)).andReturn(capabilities).once();
    EasyMock.replay(factory);
    Assertions.assertTrue(GroupByQueryEngine.canPushDownLimit(factory, DIM));
    EasyMock.verify(factory);
  }

  @Test
  public void testFilterCompactionWithLargeInList()
  {
    final DimensionSelector selector = EasyMock.createMock(DimensionSelector.class);
    EasyMock.expect(selector.getValueCardinality()).andStubReturn(10000);
    EasyMock.expect(selector.nameLookupPossibleInAdvance()).andStubReturn(true);
    EasyMock.expect(selector.idLookup()).andStubReturn((IdLookup) value -> -1);
    EasyMock.replay(selector);
    final ColumnCapabilities capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
        .setDictionaryEncoded(true).setDictionaryValuesUnique(true).setHasMultipleValues(false);
    final DefaultDimensionSpec dimension = new DefaultDimensionSpec(DIM, DIM);
    final GroupByQuery query = GroupByQuery.builder()
        .setDataSource("test")
        .setInterval(Intervals.utc(0, 1000))
        .setGranularity(Granularities.ALL)
        .setDimensions(dimension)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(Map.of("enableFilterDictionaryCompaction", true))
        .build();
    final Set<String> values = IntStream.range(0, 4097).mapToObj(String::valueOf).collect(Collectors.toSet());
    for (final DimFilter filter : List.of(
        new InDimFilter(DIM, values),
        new TypedInFilter(DIM, ColumnType.STRING, List.copyOf(values), null, null)
    )) {
      Assertions.assertEquals(values, GroupingEngine.getFilteredDimensionValues(
          query.withDimFilter(filter),
          new GroupByQueryConfig().withOverrides(query),
          dimension,
          name -> capabilities,
          selector
      ));
    }
    EasyMock.verify(selector);
  }
}
