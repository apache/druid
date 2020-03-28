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
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class MultiValuedDimensionTest extends InitializedNullHandlingTest
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

  private final GroupByQueryConfig config;
  private final ImmutableMap<String, Object> context;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public MultiValuedDimensionTest(final GroupByQueryConfig config, SegmentWriteOutMediumFactory segmentWriteOutMediumFactory, boolean forceHashAggregation)
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        ImmutableList.of(),
        config,
        null
    );
    this.config = config;
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
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags", "othertags")), null, null),
            "\t",
            ImmutableList.of("timestamp", "product", "tags", "othertags"),
            false,
            0
        ),
        "UTF-8"
    );

    String[] rows = new String[]{
        "2011-01-12T00:00:00.000Z,product_1,t1\tt2\tt3,u1\tu2",
        "2011-01-13T00:00:00.000Z,product_2,t3\tt4\tt5,u3\tu4",
        "2011-01-14T00:00:00.000Z,product_3,t5\tt6\tt7,u1\tu5",
        "2011-01-14T00:00:00.000Z,product_4,\"\",u2"
    };

    for (String row : rows) {
      incrementalIndex.add(parser.parse(row));
    }


    persistedSegmentDir = FileUtils.createTempDir();
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
    persistedSegmentDirNullSampler = FileUtils.createTempDir();
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
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970",
            "tags",
            NullHandling.replaceWithDefault() ? null : "",
            "count",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t3", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t4", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t5", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t6", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tags", "t7", "count", 2L)
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

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t3", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t4", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t5", "count", 2L)
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

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndexNullSampler, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndexNullSampler, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", null, "count", 2L)
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

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndexNullSampler, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndexNullSampler, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults;
    // an empty row e.g. [], or group by 'missing' value, is grouped with the default string value, "" or null
    // grouping input is filtered to [], null, [""]
    if (NullHandling.replaceWithDefault()) {
      // when sql compatible null handling is disabled, the inputs are effectively [], null, [null] and
      // are all grouped as null
      expectedResults = Collections.singletonList(
          GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", null, "count", 6L)
      );
    } else {
      // with sql compatible null handling, null and [] = null, but [""] = ""
      expectedResults = ImmutableList.of(
          GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", null, "count", 4L),
          GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "", "count", 2L)
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

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t3", "count", 4L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filteredDim");
  }

  @Test
  public void testGroupByExpression()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "map(x -> concat(x, 'foo'), tags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3foo", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t4foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t5foo", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t6foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t7foo", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr");
  }

  @Test
  public void testGroupByExpressionMultiMulti()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "cartesian_map((x,y) -> concat(x, y), tags, othertags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3u1", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-multi-multi");
  }

  @Test
  public void testGroupByExpressionMultiMultiAuto()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "map((x) -> concat(x, othertags), tags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3u1", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-multi-multi-auto");
  }

  @Test
  public void testGroupByExpressionMultiMultiAutoAuto()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "concat(tags, othertags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3u1", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-multi-multi-auto-auto");
  }

  @Test
  public void testGroupByExpressionMultiMultiAutoAutoDupeIdentifier()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "concat(tags, tags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimitSpec(new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec("count", OrderByColumnSpec.Direction.DESCENDING)), 5))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3t3", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t5t5", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t4t4", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2t2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t7t7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-multi-multi-auto-auto-self");
  }

  @Test
  public void testGroupByExpressionMultiMultiAutoAutoWithFilter()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "concat(tags, othertags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimFilter(new SelectorDimFilter("texpr", "t1u1", null))
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t1u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t2u2", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "texpr", "t3u1", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-multi-multi-auto-auto");
  }

  @Test
  public void testGroupByExpressionAuto()
  {
    // virtual column is a single input column and input is not used explicitly as an array,
    // so this one will work for group by v1, even with multi-value inputs
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "concat(tags, 'foo')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t1foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t2foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t3foo", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t4foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t5foo", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t6foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t7foo", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-auto");
  }

  @Test
  public void testGroupByExpressionArrayExpressionFilter()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt", ValueType.LONG))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "array_offset_of(tags, 't2')",
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", NullHandling.replaceWithDefault() ? -1L : null, "count", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", 1L, "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-auto");
  }

  @Test
  public void testGroupByExpressionArrayFnArg()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "array_to_string(map(tags -> concat('foo', tags), tags), ', ')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot1, foot2, foot3", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot3, foot4, foot5", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot5, foot6, foot7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-array-fn");
  }

  @Test
  public void testGroupByExpressionAutoArrayFnArg()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "array_to_string(concat('foo', tags), ', ')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot1, foot2, foot3", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot3, foot4, foot5", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot5, foot6, foot7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-arrayfn-auto");
  }

  @Test
  public void testGroupByExpressionFoldArrayToString()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "fold((tag, acc) -> concat(acc, tag), tags, '')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );


    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "tt",
            NullHandling.replaceWithDefault() ? null : "",
            "count",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t1t2t3", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t3t4t5", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "t5t6t7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-arrayfn-auto");
  }

  @Test
  public void testGroupByExpressionFoldArrayToStringWithConcats()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tt", "tt"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "tt",
                "fold((tag, acc) -> concat(concat(acc, case_searched(acc == '', '', ', '), concat('foo', tag)))), tags, '')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    Sequence<ResultRow> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foo", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot1, foot2, foot3", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot3, foot4, foot5", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970", "tt", "foot5, foot6, foot7", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "expr-arrayfn-auto");
  }


  @Test
  public void testGroupByExpressionMultiConflicting()
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Invalid expression: (concat [(map ([x] -> (concat [x, othertags])), [tags]), tags]); [tags] used as both scalar and array variables"
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "concat(map((x) -> concat(x, othertags), tags), tags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    ).toList();
  }

  @Test
  public void testGroupByExpressionMultiConflictingAlso()
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Invalid expression: (array_concat [tags, (array_append [othertags, tags])]); [tags] used as both scalar and array variables"
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("texpr", "texpr"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "array_concat(tags, (array_append(othertags, tags)))",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setLimit(5)
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setContext(context)
        .build();

    helper.runQueryOnSegmentsObjs(
        ImmutableList.of(
            new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
            new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("sid2"))
        ),
        query
    ).toList();
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
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("count"))
        .threshold(5)
        .filters(new SelectorDimFilter("tags", "t3", null))
        .build();

    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunnerFactory factory = new TopNQueryRunnerFactory(
          pool,
          new TopNQueryQueryToolChest(new TopNQueryConfig()),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
          factory,
          new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
          null
      );
      Sequence<Result<TopNResultValue>> result = runner.run(QueryPlus.wrap(query));
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

  @Test
  public void testTopNExpression()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("texpr", "texpr"))
        .virtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "map(x -> concat(x, 'foo'), tags)",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .metric("count")
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("count"))
        .threshold(15)
        .build();

    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunnerFactory factory = new TopNQueryRunnerFactory(
          pool,
          new TopNQueryQueryToolChest(new TopNQueryConfig()),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
          factory,
          new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
          null
      );
      Sequence<Result<TopNResultValue>> result = runner.run(QueryPlus.wrap(query));
      List<Map<String, Object>> expected =
          ImmutableList.<Map<String, Object>>builder()
                       .add(ImmutableMap.of("texpr", "t3foo", "count", 2L))
                       .add(ImmutableMap.of("texpr", "t5foo", "count", 2L))
                       .add(ImmutableMap.of("texpr", "foo", "count", 1L))
                       .add(ImmutableMap.of("texpr", "t1foo", "count", 1L))
                       .add(ImmutableMap.of("texpr", "t2foo", "count", 1L))
                       .add(ImmutableMap.of("texpr", "t4foo", "count", 1L))
                       .add(ImmutableMap.of("texpr", "t6foo", "count", 1L))
                       .add(ImmutableMap.of("texpr", "t7foo", "count", 1L))
                       .build();

      List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
          new Result<TopNResultValue>(
              DateTimes.of("2011-01-12T00:00:00.000Z"),
              new TopNResultValue(
                  expected
              )
          )
      );
      TestHelper.assertExpectedObjects(expectedResults, result.toList(), "filteredDim");
    }
  }

  @Test
  public void testTopNExpressionAutoTransform()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("texpr", "texpr"))
        .virtualColumns(
            new ExpressionVirtualColumn(
                "texpr",
                "concat(tags, 'foo')",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .metric("count")
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("count"))
        .threshold(15)
        .build();

    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunnerFactory factory = new TopNQueryRunnerFactory(
          pool,
          new TopNQueryQueryToolChest(new TopNQueryConfig()),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
          factory,
          new QueryableIndexSegment(queryableIndex, SegmentId.dummy("sid1")),
          null
      );
      Sequence<Result<TopNResultValue>> result = runner.run(QueryPlus.wrap(query));

      List<Map<String, Object>> expected =
          ImmutableList.<Map<String, Object>>builder()
              .add(ImmutableMap.of("texpr", "t3foo", "count", 2L))
              .add(ImmutableMap.of("texpr", "t5foo", "count", 2L))
              .add(ImmutableMap.of("texpr", "foo", "count", 1L))
              .add(ImmutableMap.of("texpr", "t1foo", "count", 1L))
              .add(ImmutableMap.of("texpr", "t2foo", "count", 1L))
              .add(ImmutableMap.of("texpr", "t4foo", "count", 1L))
              .add(ImmutableMap.of("texpr", "t6foo", "count", 1L))
              .add(ImmutableMap.of("texpr", "t7foo", "count", 1L))
              .build();

      List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
          new Result<TopNResultValue>(
              DateTimes.of("2011-01-12T00:00:00.000Z"),
              new TopNResultValue(
                  expected
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
