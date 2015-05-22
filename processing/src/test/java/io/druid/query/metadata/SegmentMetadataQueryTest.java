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
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentMetadataQueryTest
{
  private final SegmentMetadataQueryRunnerFactory factory = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(),
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
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(singleSegmentQueryRunner, singleSegmentQueryRunner))),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(bySegmentResult, bySegmentResult),
        myRunner.run(
            testQuery.withOverriddenContext(ImmutableMap.<String, Object>of("bySegment", true)),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata bySegment query");
    exec.shutdownNow();
  }

  @Test
  public void testSerde() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"intervals\":[\"2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z\"]\n"
                      + "}";
    Query query = mapper.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));

    // test serialize and deserialize
    Assert.assertEquals(query, mapper.readValue(mapper.writeValueAsString(query), Query.class));
  }
}
