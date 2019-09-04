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
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;

public class DoubleMeanAggregationTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper groupByQueryTestHelper;
  private final AggregationTestHelper timeseriesQueryTestHelper;

  private final List<Segment> segments;

  public DoubleMeanAggregationTest() throws Exception
  {

    groupByQueryTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Collections.EMPTY_LIST,
        new GroupByQueryConfig(),
        tempFolder
    );

    timeseriesQueryTestHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        Collections.EMPTY_LIST,
        tempFolder
    );

    segments = ImmutableList.of(
        new IncrementalIndexSegment(SimpleTestIndex.getIncrementalTestIndex(), SegmentId.dummy("test1")),
        new QueryableIndexSegment(SimpleTestIndex.getMMappedTestIndex(), SegmentId.dummy("test2"))
    );
  }

  @Test
  public void testBufferAggretatorUsingGroupByQuery() throws Exception
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", SimpleTestIndex.DOUBLE_COL),
            new DoubleMeanAggregatorFactory("meanOnString", SimpleTestIndex.SINGLE_VALUE_DOUBLE_AS_STRING_DIM),
            new DoubleMeanAggregatorFactory("meanOnMultiValue", SimpleTestIndex.MULTI_VALUE_DOUBLE_AS_STRING_DIM)
        )
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);

    Assert.assertEquals(6.2d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
    Assert.assertEquals(6.2d, result.getMetric("meanOnString").doubleValue(), 0.0001d);
    Assert.assertEquals(4.1333d, result.getMetric("meanOnMultiValue").doubleValue(), 0.0001d);
  }

  @Test
  public void testVectorAggretatorUsingGroupByQueryOnDoubleColumn() throws Exception
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.ALL)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", SimpleTestIndex.DOUBLE_COL)
        )
        .setContext(Collections.singletonMap(GroupByQueryConfig.CTX_KEY_VECTORIZE, true))
        .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = groupByQueryTestHelper.getObjectMapper();
    query = (GroupByQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence<ResultRow> seq = groupByQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    Row result = Iterables.getOnlyElement(seq.toList()).toMapBasedRow(query);

    Assert.assertEquals(6.2d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
  }

  @Test
  public void testAggretatorUsingTimeseriesQuery() throws Exception
  {
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
                                  .build();

    // do json serialization and deserialization of query to ensure there are no serde issues
    ObjectMapper jsonMapper = timeseriesQueryTestHelper.getObjectMapper();
    query = (TimeseriesQuery) jsonMapper.readValue(jsonMapper.writeValueAsString(query), Query.class);

    Sequence seq = timeseriesQueryTestHelper.runQueryOnSegmentsObjs(segments, query);
    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();

    Assert.assertEquals(6.2d, result.getDoubleMetric("meanOnDouble").doubleValue(), 0.0001d);
    Assert.assertEquals(6.2d, result.getDoubleMetric("meanOnString").doubleValue(), 0.0001d);
    Assert.assertEquals(4.1333d, result.getDoubleMetric("meanOnMultiValue").doubleValue(), 0.0001d);
  }
}
