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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class MultiValuedDimensionTest
{
  @Parameterized.Parameters(name = "groupby: {0} forceHashAggregation: {2} ({1})")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config, TmpFileSegmentWriteOutMediumFactory.instance(), false});
      constructors.add(new Object[]{config, OffHeapMemorySegmentWriteOutMediumFactory.instance(), false});
      constructors.add(new Object[]{config, TmpFileSegmentWriteOutMediumFactory.instance(), true});
      constructors.add(new Object[]{config, OffHeapMemorySegmentWriteOutMediumFactory.instance(), true});
    }
    return constructors;
  }

  private final AggregationTestHelper helper;
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  private IncrementalIndex incrementalIndex;
  private QueryableIndex queryableIndex;

  private File persistedSegmentDir;

  private IncrementalIndex incrementalIndexNullSampler;
  private QueryableIndex queryableIndexNullSampler;
  private File persistedSegmentDirNullSampler;

  private final ImmutableMap<String, Object> context;

  public MultiValuedDimensionTest(final GroupByQueryConfig config, SegmentWriteOutMediumFactory segmentWriteOutMediumFactory, boolean forceHashAggregation)
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        ImmutableList.of(),
        config,
        null
    );
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;

    this.context = config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)
                   ? ImmutableMap.of()
                   : ImmutableMap.of("forceHashAggregation", forceHashAggregation);
  }

  @Before
  public void setup() throws Exception
  {
    incrementalIndex = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(5000)
        .buildOnheap();

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags")), null, null),
            "\t",
            ImmutableList.of("timestamp", "product", "tags"),
            false,
            0
        ),
        "UTF-8"
    );

    String[] rows = new String[]{
        "2011-01-12T00:00:00.000Z,product_1,t1\tt2\tt3",
        "2011-01-13T00:00:00.000Z,product_2,t3\tt4\tt5",
        "2011-01-14T00:00:00.000Z,product_3,t5\tt6\tt7",
        "2011-01-14T00:00:00.000Z,product_4"
    };

    for (String row : rows) {
      incrementalIndex.add(parser.parse(row));
    }

    persistedSegmentDir = Files.createTempDir();
    TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory)
              .persist(incrementalIndex, persistedSegmentDir, new IndexSpec(), null);

    queryableIndex = TestHelper.getTestIndexIO().loadIndex(persistedSegmentDir);



    StringInputRowParser parserNullSampler = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("time", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags", "othertags")), null, null)
        ),
        "UTF-8"
    );

    incrementalIndexNullSampler = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(5000)
        .buildOnheap();

    String[] rowsNullSampler = new String[]{
        "{\"time\":\"2011-01-13T00:00:00.000Z\",\"product\":\"product_1\",\"tags\":[],\"othertags\":[\"u1\", \"u2\"]}",
        "{\"time\":\"2011-01-12T00:00:00.000Z\",\"product\":\"product_2\",\"othertags\":[\"u3\", \"u4\"]}",
        "{\"time\":\"2011-01-14T00:00:00.000Z\",\"product\":\"product_3\",\"tags\":[\"\"],\"othertags\":[\"u1\", \"u5\"]}",
        "{\"time\":\"2011-01-15T00:00:00.000Z\",\"product\":\"product_4\",\"tags\":[\"t1\", \"t2\", \"\"],\"othertags\":[\"u6\", \"u7\"]}",
        "{\"time\":\"2011-01-16T00:00:00.000Z\",\"product\":\"product_5\",\"tags\":[],\"othertags\":[]}",
        "{\"time\":\"2011-01-16T00:00:00.000Z\",\"product\":\"product_6\"}",
        "{\"time\":\"2011-01-16T00:00:00.000Z\",\"product\":\"product_7\",\"othertags\":[]}",
        "{\"time\":\"2011-01-16T00:00:00.000Z\",\"product\":\"product_8\",\"tags\":[\"\"],\"othertags\":[]}"
    };

    for (String row : rowsNullSampler) {
      incrementalIndexNullSampler.add(parserNullSampler.parse(row));
    }
    persistedSegmentDirNullSampler = Files.createTempDir();
    TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory)
              .persist(incrementalIndexNullSampler, persistedSegmentDirNullSampler, new IndexSpec(), null);

    queryableIndexNullSampler = TestHelper.getTestIndexIO().loadIndex(persistedSegmentDirNullSampler);
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testGroupByNoFilter()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tags", "tags"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", null, "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t3", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t4", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t5", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t6", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "noFilter");
  }

  @Test
  public void testGroupByWithDimFilter()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tags", "tags"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setDimFilter(new SelectorDimFilter("tags", "t3", null))
        .setContext(context)
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t3", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t4", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t5", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "dimFilter");
  }

  @Test
  public void testGroupByWithDimFilterEmptyResults()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tags", "tags"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setDimFilter(new InDimFilter("product", ImmutableList.of("product_5"), null))
        .setContext(context)
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndexNullSampler, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndexNullSampler, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", null, "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filter-empty");
  }

  @Test
  public void testGroupByWithDimFilterNullishResults()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tags", "tags"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setDimFilter(
            new InDimFilter("product", ImmutableList.of("product_5", "product_6", "product_8"), null)
        )
        .setContext(context)
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndexNullSampler, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndexNullSampler, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<Row> expectedResults;
    // an empty row e.g. [], or group by 'missing' value, is grouped with the default string value, "" or null
    // grouping input is filtered to [], null, [""]
    if (NullHandling.replaceWithDefault()) {
      // when sql compatible null handling is disabled, the inputs are effectively [], null, [null] and
      // are all grouped as null
      expectedResults = Collections.singletonList(
          GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", null, "count", 6L)
      );
    } else {
      // with sql compatible null handling, null and [] = null, but [""] = ""
      expectedResults = ImmutableList.of(
          GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", null, "count", 4L),
          GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "", "count", 2L)
      );
    }

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filter-nullish");
  }

  @Test
  public void testGroupByWithDimFilterAndWithFilteredDimSpec()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new RegexFilteredDimensionSpec(new DefaultDimensionSpec("tags", "tags"), "t3"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setDimFilter(new SelectorDimFilter("tags", "t3", null))
        .setContext(context)
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t3", "count", 4L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filteredDim");
  }

  @Test
  public void testTopNWithDimFilterAndWithFilteredDimSpec()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new ListFilteredDimensionSpec(
            new DefaultDimensionSpec("tags", "tags"),
            ImmutableSet.of("t3"),
            null
        ))
        .metric("count")
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
        .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
        .threshold(5)
        .filters(new SelectorDimFilter("tags", "t3", null))
        .build();

    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunnerFactory factory = new TopNQueryRunnerFactory(
          pool,
          new TopNQueryQueryToolChest(
              new TopNQueryConfig(),
              QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
          ),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
          factory,
          new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
          null
      );
      Map<String, Object> context = new HashMap<>();
      Sequence<Result<TopNResultValue>> result = runner.run(QueryPlus.wrap(query), context);
      List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
          new Result<TopNResultValue>(
              DateTimes.of("2011-01-12T00:00:00.000Z"),
              new TopNResultValue(
                  Collections.<Map<String, Object>>singletonList(
                      ImmutableMap.of(
                          "tags", "t3",
                          "count", 2L
                      )
                  )
              )
          )
      );
      TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filteredDim");
    }
  }

  @After
  public void cleanup() throws Exception
  {
    queryableIndex.close();
    incrementalIndex.close();
    FileUtils.deleteDirectory(persistedSegmentDir);
  }
}
