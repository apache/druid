/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.JodaUtils;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.ValueType;
import io.druid.timeline.LogicalSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentMetadataQueryTest
{
  private final SegmentMetadataQueryRunnerFactory factory = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  @SuppressWarnings("unchecked")
  private final QueryRunner runner = makeQueryRunner(factory);

  private final ObjectMapper mapper = new DefaultObjectMapper();

  @SuppressWarnings("unchecked")
  public static QueryRunner makeQueryRunner(
      QueryRunnerFactory factory
  )
  {
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        new QueryableIndexSegment(QueryRunnerTestHelper.segmentId, TestIndex.getMMappedTestIndex())
    );
  }

  private final SegmentMetadataQuery testQuery;
  private final SegmentAnalysis expectedSegmentAnalysis;

  public SegmentMetadataQueryTest()
  {
    testQuery = Druids.newSegmentMetadataQueryBuilder()
                      .dataSource("testing")
                      .intervals("2013/2014")
                      .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                      .merge(true)
                      .build();

    expectedSegmentAnalysis = new SegmentAnalysis(
        "testSegment",
        ImmutableList.of(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                10881,
                1,
                null
            )
        ), 71982
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSegmentMetadataQuery()
  {
    List<SegmentAnalysis> results = Sequences.toList(
        runner.run(testQuery, Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expectedSegmentAnalysis), results);
  }

  @Test
  public void testBySegmentResults()
  {
    Result<BySegmentResultValue> bySegmentResult = new Result<BySegmentResultValue>(
        expectedSegmentAnalysis.getIntervals().get(0).getStart(),
        new BySegmentResultValueClass(
            Arrays.asList(
                expectedSegmentAnalysis
            ), expectedSegmentAnalysis.getId(), testQuery.getIntervals().get(0)
        )
    );

    QueryToolChest toolChest = factory.getToolchest();

    QueryRunner singleSegmentQueryRunner = toolChest.preMergeQueryDecoration(runner);
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(
                Executors.newCachedThreadPool(),
                //Note: It is essential to have atleast 2 query runners merged to reproduce the regression bug described in
                //https://github.com/druid-io/druid/pull/1172
                //the bug surfaces only when ordering is used which happens only when you have 2 things to compare
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(singleSegmentQueryRunner, singleSegmentQueryRunner)
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(bySegmentResult, bySegmentResult),
        myRunner.run(
            testQuery.withOverriddenContext(ImmutableMap.<String, Object>of("bySegment", true)),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata bySegment query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSerde() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"intervals\":[\"2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z\"],\n"
                      + "  \"analysisTypes\":[\"cardinality\",\"size\"]\n"
                      + "}";

    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysisTypes = EnumSet.of(
        SegmentMetadataQuery.AnalysisType.CARDINALITY,
        SegmentMetadataQuery.AnalysisType.SIZE
    );

    Query query = mapper.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));
    Assert.assertEquals(expectedAnalysisTypes, ((SegmentMetadataQuery) query).getAnalysisTypes());

    // test serialize and deserialize
    Assert.assertEquals(query, mapper.readValue(mapper.writeValueAsString(query), Query.class));
  }

  @Test
  public void testSerdeWithDefaultInterval() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\"\n"
                      + "}";
    Query query = mapper.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT), query.getIntervals().get(0));
    Assert.assertTrue(((SegmentMetadataQuery) query).isUsingDefaultInterval());

    // test serialize and deserialize
    Assert.assertEquals(query, mapper.readValue(mapper.writeValueAsString(query), Query.class));
  }

  @Test
  public void testDefaultIntervalAndFiltering() throws Exception
  {
    SegmentMetadataQuery testQuery = Druids.newSegmentMetadataQueryBuilder()
                                           .dataSource("testing")
                                           .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                           .merge(true)
                                           .build();

    Interval expectedInterval = new Interval(
        JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT
    );

    /* No interval specified, should use default interval */
    Assert.assertTrue(testQuery.isUsingDefaultInterval());
    Assert.assertEquals(testQuery.getIntervals().get(0), expectedInterval);
    Assert.assertEquals(testQuery.getIntervals().size(), 1);

    List<LogicalSegment> testSegments = Arrays.asList(
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
            return new Interval("2013-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-05-20/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-02-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
          }
        }
    );

    /* Test default period filter */
    List<LogicalSegment> filteredSegments = new SegmentMetadataQueryQueryToolChest(
        new SegmentMetadataQueryConfig()
    ).filterSegments(
        testQuery,
        testSegments
    );

    List<LogicalSegment> expectedSegments = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
          }
        }
    );

    Assert.assertEquals(filteredSegments.size(), 2);
    for (int i = 0; i < filteredSegments.size(); i++) {
      Assert.assertEquals(expectedSegments.get(i).getInterval(), filteredSegments.get(i).getInterval());
    }

    /* Test 2 year period filtering */
    SegmentMetadataQueryConfig twoYearPeriodCfg = new SegmentMetadataQueryConfig("P2Y");
    List<LogicalSegment> filteredSegments2 = new SegmentMetadataQueryQueryToolChest(
        twoYearPeriodCfg
    ).filterSegments(
        testQuery,
        testSegments
    );

    List<LogicalSegment> expectedSegments2 = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-05-20/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-02-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
          }
        }
    );

    Assert.assertEquals(filteredSegments2.size(), 5);
    for (int i = 0; i < filteredSegments2.size(); i++) {
      Assert.assertEquals(expectedSegments2.get(i).getInterval(), filteredSegments2.get(i).getInterval());
    }
  }

  @Test
  public void testCacheKeyWithListColumnIncluderator()
  {
    SegmentMetadataQuery oneColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("foo")))
                                                .build();

    SegmentMetadataQuery twoColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("fo", "o")))
                                                .build();

    final byte[] oneColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(null).getCacheStrategy(oneColumnQuery)
                                                                                      .computeCacheKey(oneColumnQuery);

    final byte[] twoColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(null).getCacheStrategy(twoColumnQuery)
                                                                                      .computeCacheKey(twoColumnQuery);

    Assert.assertFalse(Arrays.equals(oneColumnQueryCacheKey, twoColumnQueryCacheKey));
  }
}
