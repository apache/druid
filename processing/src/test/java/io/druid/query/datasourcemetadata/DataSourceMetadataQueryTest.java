/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.datasourcemetadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSourceMetadataQueryTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newDataSourceMetadataQueryBuilder()
                        .dataSource("testing")
                        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final DataSourceMetadataQuery query = Druids.newDataSourceMetadataQueryBuilder()
                                                .dataSource("foo")
                                                .intervals("2013/2014")
                                                .context(
                                                    ImmutableMap.<String, Object>of(
                                                        "priority",
                                                        1,
                                                        "useCache",
                                                        true,
                                                        "populateCache",
                                                        "true",
                                                        "finalize",
                                                        true
                                                    )
                                                ).build();

    final ObjectMapper mapper = new DefaultObjectMapper();

    final Query serdeQuery = mapper.readValue(
        mapper.writeValueAsBytes(
            mapper.readValue(
                mapper.writeValueAsString(
                    query
                ), Query.class
            )
        ), Query.class
    );

    Assert.assertEquals(1, serdeQuery.getContextValue("priority"));
    Assert.assertEquals(true, serdeQuery.getContextValue("useCache"));
    Assert.assertEquals("true", serdeQuery.getContextValue("populateCache"));
    Assert.assertEquals(true, serdeQuery.getContextValue("finalize"));
    Assert.assertEquals(true, serdeQuery.getContextBoolean("useCache", false));
    Assert.assertEquals(true, serdeQuery.getContextBoolean("populateCache", false));
    Assert.assertEquals(true, serdeQuery.getContextBoolean("finalize", false));
  }

  @Test
  public void testMaxIngestedEventTime() throws Exception
  {
    final IncrementalIndex rtIndex = new OnheapIncrementalIndex(
        0L, QueryGranularities.NONE, new AggregatorFactory[]{new CountAggregatorFactory("count")}, 1000
    );
    ;
    final QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        (QueryRunnerFactory) new DataSourceMetadataQueryRunnerFactory(
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ), new IncrementalIndexSegment(rtIndex, "test"),
        null
    );
    DateTime timestamp = new DateTime(System.currentTimeMillis());
    rtIndex.add(
        new MapBasedInputRow(
            timestamp.getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "x")
        )
    );
    DataSourceMetadataQuery dataSourceMetadataQuery = Druids.newDataSourceMetadataQueryBuilder()
                                                            .dataSource("testing")
                                                            .build();
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    Iterable<Result<DataSourceMetadataResultValue>> results = Sequences.toList(
        runner.run(dataSourceMetadataQuery, context),
        Lists.<Result<DataSourceMetadataResultValue>>newArrayList()
    );
    DataSourceMetadataResultValue val = results.iterator().next().getValue();
    DateTime maxIngestedEventTime = val.getMaxIngestedEventTime();

    Assert.assertEquals(timestamp, maxIngestedEventTime);
  }

  @Test
  public void testFilterSegments()
  {
    List<LogicalSegment> segments = new DataSourceQueryQueryToolChest().filterSegments(
        null,
        Arrays.asList(
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2012-01-01/P1D");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2012-01-01T01/PT1H");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01/P1D");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T01/PT1H");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T02/PT1H");
              }
            }
        )
    );

    Assert.assertEquals(segments.size(), 2);
    // should only have the latest segments. 
    List<LogicalSegment> expected = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T02/PT1H");
          }
        }
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(),segments.get(i).getInterval());
    }
  }

  @Test
  public void testResultSerialization()
  {
    final DataSourceMetadataResultValue resultValue = new DataSourceMetadataResultValue(new DateTime("2000-01-01T00Z"));
    final Map<String, Object> resultValueMap = new DefaultObjectMapper().convertValue(
        resultValue,
        new TypeReference<Map<String, Object>>()
        {
        }
    );
    Assert.assertEquals(
        ImmutableMap.<String, Object>of("maxIngestedEventTime", "2000-01-01T00:00:00.000Z"),
        resultValueMap
    );
  }

  @Test
  public void testResultDeserialization()
  {
    final Map<String, Object> resultValueMap = ImmutableMap.<String, Object>of(
        "maxIngestedEventTime",
        "2000-01-01T00:00:00.000Z"
    );
    final DataSourceMetadataResultValue resultValue = new DefaultObjectMapper().convertValue(
        resultValueMap,
        DataSourceMetadataResultValue.class
    );
    Assert.assertEquals(new DateTime("2000"), resultValue.getMaxIngestedEventTime());
  }

}
