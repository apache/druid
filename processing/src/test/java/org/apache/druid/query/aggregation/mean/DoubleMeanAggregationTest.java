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

package org.apache.druid.query.aggregation.mean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;

public class DoubleMeanAggregationTest
{
  public static Object[] doVectorizeAndUseVectorApi()
  {
    return new Object[]{
        new Object[]{false, false},
        new Object[]{true, false},
        new Object[]{true, true}
    };
  }

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = TemporaryFolderExtension.testCaseScoped();

  private final AggregationTestHelper groupByQueryTestHelper;
  private final AggregationTestHelper timeseriesQueryTestHelper;

  private final List<Segment> segments;
  private final List<Segment> biggerSegments;

  public DoubleMeanAggregationTest()
  {

    groupByQueryTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Collections.emptyList(),
        new GroupByQueryConfig(),
        tempFolder
    );

    timeseriesQueryTestHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        Collections.emptyList(),
        tempFolder
    );

    segments = ImmutableList.of(
        new IncrementalIndexSegment(SimpleTestIndex.getIncrementalTestIndex(), SegmentId.dummy("test1")),
        new QueryableIndexSegment(SimpleTestIndex.getMMappedTestIndex(), SegmentId.dummy("test2"))
    );

    biggerSegments = ImmutableList.of(
        new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SegmentId.dummy("test1")),
        new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test2"))
    );
  }

  @AfterEach
  public void resetExpressionProcessing()
  {
    ExpressionProcessing.initializeForTests();
  }

  @ParameterizedTest
  @MethodSource("doVectorizeAndUseVectorApi")
  public void testBufferAggretatorUsingGroupByQuery(final boolean doVectorize, final boolean useVectorApi)
      throws Exception
  {
    initializeExpressionProcessing(useVectorApi);

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", SimpleTestIndex.DOUBLE_COL),
            new DoubleMeanAggregatorFactory("meanOnString", SimpleTestIndex.SINGLE_VALUE_DOUBLE_AS_STRING_DIM),
            new DoubleMeanAggregatorFactory("meanOnMultiValue", SimpleTestIndex.MULTI_VALUE_DOUBLE_AS_STRING_DIM)
        )
        .setContext(ImmutableMap.of(QueryContextParameters.VECTORIZE.getName(), doVectorize))
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);

    Assertions.assertEquals(6.2d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
    Assertions.assertEquals(6.2d, result.getMetric("meanOnString").doubleValue(), 0.0001d);
    Assertions.assertEquals(4.1333d, result.getMetric("meanOnMultiValue").doubleValue(), 0.0001d);
  }

  @ParameterizedTest
  @MethodSource("doVectorizeAndUseVectorApi")
  public void testVectorAggretatorUsingGroupByQueryOnDoubleColumn(
      final boolean doVectorize,
      final boolean useVectorApi
  )
      throws Exception
  {
    initializeExpressionProcessing(useVectorApi);

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", SimpleTestIndex.DOUBLE_COL)
        )
        .setContext(Collections.singletonMap(QueryContextParameters.VECTORIZE.getName(), doVectorize))
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);

    Assertions.assertEquals(6.2d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
  }

  @ParameterizedTest
  @MethodSource("doVectorizeAndUseVectorApi")
  public void testVectorAggretatorUsingGroupByQueryOnDoubleColumnOnBiggerSegments(
      final boolean doVectorize,
      final boolean useVectorApi
  ) throws Exception
  {
    initializeExpressionProcessing(useVectorApi);

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("blah")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", TestIndex.COLUMNS[9])
        )
        .setContext(Collections.singletonMap(QueryContextParameters.VECTORIZE.getName(), doVectorize))
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(biggerSegments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);
    Assertions.assertEquals(51.0d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
  }

  @ParameterizedTest
  @MethodSource("doVectorizeAndUseVectorApi")
  public void testAggretatorUsingTimeseriesQuery(final boolean doVectorize, final boolean useVectorApi)
      throws Exception
  {
    initializeExpressionProcessing(useVectorApi);

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(Granularities.ALL)
                                  .intervals("1970/2050")
                                  .aggregators(
                                      new DoubleMeanAggregatorFactory("meanOnDouble", SimpleTestIndex.DOUBLE_COL),
                                      new DoubleMeanAggregatorFactory(
                                          "meanOnString",
                                          SimpleTestIndex.SINGLE_VALUE_DOUBLE_AS_STRING_DIM
                                      ),
                                      new DoubleMeanAggregatorFactory(
                                          "meanOnMultiValue",
                                          SimpleTestIndex.MULTI_VALUE_DOUBLE_AS_STRING_DIM
                                      )
                                  )
                                  .context(ImmutableMap.of(QueryContextParameters.VECTORIZE.getName(), doVectorize))
                                  .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = timeseriesQueryTestHelper.getObjectMapper();
    query = (TimeseriesQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence seq = timeseriesQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();

    Assertions.assertEquals(6.2d, result.getDoubleMetric("meanOnDouble").doubleValue(), 0.0001d);
    Assertions.assertEquals(6.2d, result.getDoubleMetric("meanOnString").doubleValue(), 0.0001d);
    Assertions.assertEquals(4.1333d, result.getDoubleMetric("meanOnMultiValue").doubleValue(), 0.0001d);
  }

  @Test
  public void testAggregateWithSize()
  {
    Double[] values = new Double[]{3.0, 1.0, 2.0};
    TestObjectColumnSelector<Double> columnValueSelector = new TestObjectColumnSelector<>(values);

    ColumnSelectorFactory colSelectorFactory = EasyMock.mock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeColumnValueSelector(EasyMock.anyString()))
            .andReturn(columnValueSelector).anyTimes();
    EasyMock.replay(colSelectorFactory);

    DoubleMeanAggregatorFactory aggregatorFactory = new DoubleMeanAggregatorFactory("name", "fieldName");
    AggregatorAndSize aggregatorAndSize = aggregatorFactory.factorizeWithSize(colSelectorFactory);

    Assertions.assertEquals(
        aggregatorFactory.getMaxIntermediateSize(),
        aggregatorAndSize.getInitialSizeBytes()
    );
    Assertions.assertTrue(aggregatorAndSize.getAggregator() instanceof DoubleMeanAggregator);
    Aggregator aggregator = aggregatorAndSize.getAggregator();
    for (int i = 0; i < values.length; ++i) {
      long sizeDelta = aggregator.aggregateWithSize();
      Assertions.assertEquals(0L, sizeDelta);
      columnValueSelector.increment();
    }

    DoubleMeanHolder meanHolder = (DoubleMeanHolder) aggregator.get();
    Assertions.assertEquals(2.0, meanHolder.mean(), 0.0);
  }

  private static void initializeExpressionProcessing(final boolean useVectorApi)
  {
    if (useVectorApi) {
      ExpressionProcessing.initializeForVectorApiTests();
    } else {
      ExpressionProcessing.initializeForTests();
    }
  }
}
