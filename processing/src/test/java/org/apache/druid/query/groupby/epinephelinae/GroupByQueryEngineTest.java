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

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.util.List;

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

  @ParameterizedTest
  @CsvSource({
      "3,4,false,true,4096,4096,20",
      "3,4,false,true,1,4096,-1",
      "2147483647,4,false,true,4096,4096,-1",
      "-1,4,false,true,4096,4096,-1",
      "3,4,true,true,4096,4096,-1",
      "3,4,false,false,4096,4096,-1",
      "63,63,false,true,65536,4096,4096",
      "63,64,false,true,65536,4096,-1",
      "3,4,false,true,4096,19,-1",
      "3,4,false,true,4096,20,20",
      "3,4,false,true,4096,0,-1",
      "1000,1000,false,true,16000000,4096,-1"
  })
  public void testMultipleDimensionArrayAggregationCardinality(
      final int firstCardinality,
      final int secondCardinality,
      final boolean multiValue,
      final boolean uniqueDictionary,
      final int bufferSize,
      final int maxCardinality,
      final int expectedCardinality
  )
  {
    final GroupByQuery query = GroupByQuery.builder()
        .setDataSource("test")
        .setInterval(new Interval(0, 1000))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("d0", "d0"), new DefaultDimensionSpec("d1", "d1"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .build();
    final ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
        .setDictionaryEncoded(true).setDictionaryValuesUnique(uniqueDictionary).setHasMultipleValues(multiValue);
    final GroupByQueryConfig config = new GroupByQueryConfig()
    {
      @Override
      public int getMaxMultiDimensionArrayCardinality()
      {
        return maxCardinality;
      }
    };
    Assertions.assertEquals(expectedCardinality, GroupingEngine.getCardinalityForArrayAggregation(
        config,
        query,
        name -> capabilities,
        List.of(selector(firstCardinality), selector(secondCardinality)),
        ByteBuffer.allocate(bufferSize)
    ));
  }

  private static GroupingSelector selector(final int cardinality)
  {
    return new GroupingSelector()
    {
      @Override
      public int getValueCardinality()
      {
        return cardinality;
      }
    };
  }

}
