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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.LookupExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;
  private Supplier<GroupByQueryConfig> configSupplier;
  private ValueType groupingKeyType;
  private String qualityName;
  private String marketName;

  private static final Map<String, String> QUALITY_RENAMING_MAP = ImmutableMap.<String, String>builder()
      .put("automotive", "automotive0")
      .put("business", "business0")
      .put("entertainment", "entertainment0")
      .put("health", "health0")
      .put("mezzanine", "mezzanine0")
      .put("news", "news0")
      .put("premium", "premium0")
      .put("technology", "technology0")
      .put("travel", "travel0")
      .put("1111", "automotive0")
      .put("2222", "business0")
      .put("3333", "entertainment0")
      .put("4444", "health0")
      .put("5555", "mezzanine0")
      .put("6666", "news0")
      .put("7777", "premium0")
      .put("8888", "technology0")
      .put("8899", "travel0")
      .put("111.111", "automotive0")
      .put("222.222", "business0")
      .put("333.333", "entertainment0")
      .put("444.444", "health0")
      .put("555.555", "mezzanine0")
      .put("666.666", "news0")
      .put("777.777", "premium0")
      .put("888.888", "technology0")
      .put("889.999", "travel0")
      .build();

  @Before
  public void setUp() throws Exception
  {
    configSupplier = Suppliers.ofInstance(new GroupByQueryConfig());
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(
            configSupplier, mapper, engine, TestQueryRunners.pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        TestQueryRunners.pool
    );

    GroupByQueryConfig singleThreadedConfig = new GroupByQueryConfig()
    {
      @Override
      public boolean isSingleThreaded()
      {
        return true;
      }
    };
    singleThreadedConfig.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> singleThreadedConfigSupplier = Suppliers.ofInstance(singleThreadedConfig);
    final GroupByQueryEngine singleThreadEngine = new GroupByQueryEngine(singleThreadedConfigSupplier, pool);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        singleThreadEngine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        singleThreadedConfigSupplier,
        new GroupByQueryQueryToolChest(
            singleThreadedConfigSupplier, mapper, singleThreadEngine, pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        pool
    );


    Function<Object, List<Object[]>> function = new Function<Object, List<Object[]>>()
    {
      @Override
      public List<Object[]> apply(@Nullable Object input)
      {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{factory, input, ValueType.STRING});
        params.add(new Object[]{factory, input, ValueType.LONG});
        params.add(new Object[]{factory, input, ValueType.FLOAT});
        return params;
      }
    };

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.concat(
                Iterables.transform(
                    QueryRunnerTestHelper.makeQueryRunners(factory),
                    function
                )
            ),
            Iterables.concat(
                Iterables.transform(
                    QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                    function
                )
            )
        )
    );
  }

  private Object getDimMapping(String value) {
    return TestIndex.getDimMappingForType(groupingKeyType, value);
  }

  private String getDimMappingFirstChar(String value) {
    return getDimMapping(value).toString().substring(0,1);
  }

  public GroupByQueryRunnerTest(GroupByQueryRunnerFactory factory, QueryRunner runner, ValueType groupingKeyType)
  {
    this.factory = factory;
    this.runner = runner;
    this.groupingKeyType = groupingKeyType;
    this.qualityName = (String) getDimMapping("quality");
    this.marketName = (String) getDimMapping("market");
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithRebucketRename()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(QUALITY_RENAMING_MAP), false, null, false, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissingNonInjective()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(QUALITY_RENAMING_MAP), true, null, false, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissing()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(QUALITY_RENAMING_MAP), true, null, true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithSimpleRenameAndMissingString()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(QUALITY_RENAMING_MAP), false, "MISSING", true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithSimpleRename()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(new MapLookupExtractor(QUALITY_RENAMING_MAP), false, null, true, false),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithUniquesAndPostAggWithSameName()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new HyperUniquesAggregatorFactory(
                    "quality_uniques",
                    "quality_uniques"
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "quality_uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.qualityCardinality
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithNullProducingDimExtractionFn()
  {
    final ExtractionFn nullExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals(getDimMapping("mezzanine").toString()) ? null : super.apply(dimValue);
      }
    };
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(qualityName,
                                            "alias",
                                            nullExtractionFn,
                                            null)
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("technology"), "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("technology"), "rows", 2L, "idx", 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        ""
    );
  }

  @Test
  /**
   * This test exists only to show what the current behavior is and not necessarily to define that this is
   * correct behavior.  In fact, the behavior when returning the empty string from a DimExtractionFn is, by
   * contract, undefined, so this can do anything.
   */
  public void testGroupByWithEmptyStringProducingDimExtractionFn()
  {
    final ExtractionFn emptyStringExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return dimValue.equals(getDimMapping("mezzanine").toString()) ? "" : super.apply(dimValue);
      }
    };

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(qualityName, "alias", emptyStringExtractionFn, null)
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMappingFirstChar("technology"), "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMappingFirstChar("technology"), "rows", 2L, "idx", 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        ""
    );
  }

  @Test
  public void testGroupByWithTimeZone()
  {
    DateTimeZone tz = DateTimeZone.forID("America/Los_Angeles");

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                     .setDimensions(
                                         Lists.newArrayList(
                                             (DimensionSpec) new DefaultDimensionSpec(
                                                 qualityName,
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory(
                                                 "idx",
                                                 "index"
                                             )
                                         )
                                     )
                                     .setGranularity(
                                         new PeriodGranularity(
                                             new Period("P1D"),
                                             null,
                                             tz
                                         )
                                     )
                                     .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx",
            135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx",
            118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx",
            120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            3L,
            "idx",
            2870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx",
            121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("premium"),
            "rows",
            3L,
            "idx",
            2900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx",
            78L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx",
            119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx",
            147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx",
            112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx",
            113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            3L,
            "idx",
            2447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx",
            114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("premium"),
            "rows",
            3L,
            "idx",
            2505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping( "technology"),
            "rows",
            1L,
            "idx",
            97L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx",
            126L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testMergeResults()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();
    final GroupByQuery allGranQuery = builder.copy().setGranularity(QueryGranularity.ALL).build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1, responseContext), runner.run(query2, responseContext));
          }
        }
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(fullQuery, context), "direct");
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(allGranExpectedResults, runner.run(allGranQuery, context), "direct");
    TestHelper.assertExpectedObjects(allGranExpectedResults, mergedRunner.run(allGranQuery, context), "merged");
  }

  @Test
  public void testMergeResultsWithLimit()
  {
    for (int limit = 1; limit < 20; ++limit) {
      doTestMergeResultsWithValidLimit(limit);
    }
  }

  private void doTestMergeResultsWithValidLimit(final int limit)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(Integer.valueOf(limit));

    final GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), mergeRunner.run(fullQuery, context), String.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderBy()
  {
    final int limit = 14;
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryGranularity.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING);

    final GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 1L, "idx", 78L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 1L, "idx", 126L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit), mergeRunner.run(fullQuery, context), String.format("limit: %d", limit)
    );
  }


  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(Integer.valueOf(-1));

    builder.build();
  }

  @Test
  public void testMergeResultsWithOrderBy()
  {
    LimitSpec[] orderBySpecs = new LimitSpec[]{
        new DefaultLimitSpec(OrderByColumnSpec.ascending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.ascending("rows", "idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("rows", "idx"), null),
        };

    final Comparator<Row> idxComparator =
        new Comparator<Row>()
        {
          @Override
          public int compare(Row o1, Row o2)
          {
            return Float.compare(o1.getFloatMetric("idx"), o2.getFloatMetric("idx"));
          }
        };

    Comparator<Row> rowsIdxComparator =
        new Comparator<Row>()
        {

          @Override
          public int compare(Row o1, Row o2)
          {
            int value = Float.compare(o1.getFloatMetric("rows"), o2.getFloatMetric("rows"));
            if (value != 0) {
              return value;
            }

            return idxComparator.compare(o1, o2);
          }
        };

    List<Row> allResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L)
    );

    List<List<Row>> expectedResults = Lists.newArrayList(
        Ordering.from(idxComparator).sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).sortedCopy(allResults),
        Ordering.from(idxComparator).reverse().sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).reverse().sortedCopy(allResults)
    );

    for (int i = 0; i < orderBySpecs.length; ++i) {
      doTestMergeResultsWithOrderBy(orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(LimitSpec orderBySpec, List<Row> expectedResults)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(orderBySpec);

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1, responseContext), runner.run(query2, responseContext));
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testGroupByOrderLimit() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L)
    );

    Map<String, Object> context = Maps.newHashMap();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit2() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("rows", "desc")
        .addOrderByColumn("alias", "d")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 2L, "idx", 269L)
    );

    Map<String, Object> context = Maps.newHashMap();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit3() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx", "index")
            )
        )
        .addOrderByColumn("idx", "desc")
        .addOrderByColumn("alias", "d")
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            6L,
            "idx",
            4423.6533203125D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            6L,
            "idx",
            4418.61865234375D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            2L,
            "idx",
            319.94403076171875D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("automotive"),
            "rows",
            2L,
            "idx",
            270.3977966308594D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("travel"),
            "rows",
            2L,
            "idx",
            243.65843200683594D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("news"),
            "rows",
            2L,
            "idx",
            222.20980834960938D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("business"),
            "rows",
            2L,
            "idx",
            218.7224884033203D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("health"),
            "rows",
            2L,
            "idx",
            216.97836303710938D
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("technology"),
            "rows",
            2L,
            "idx",
            178.24917602539062D
        )
    );

    Map<String, Object> context = Maps.newHashMap();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5), mergeRunner.run(builder.limit(5).build(), context), "limited"
    );
  }

  @Test
  public void testGroupByWithSameCaseOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    marketName,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            getDimMapping("upfront"),
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            getDimMapping("total_market"),
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            getDimMapping("spot"),
            "rows",
            837L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    marketName,
                    marketName
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        marketName,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 3
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", marketName, getDimMapping("upfront"), "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            marketName,
            getDimMapping("total_market"),
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", marketName, getDimMapping("spot"), "rows", 837L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Ignore
  @Test
  // This is a test to verify per limit groupings, but Druid currently does not support this functionality. At a point
  // in time when Druid does support this, we can re-evaluate this test.
  public void testLimitPerGrouping()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    marketName,
                    marketName
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.firstToThird)
        // Using a limitSpec here to achieve a per group limit is incorrect.
        // Limit is applied on the overall results.
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "rows",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 2
            )
        )
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01T00:00:00.000Z", marketName, getDimMapping("spot"), "rows", 9L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02T00:00:00.000Z", marketName, getDimMapping("spot"), "rows", 9L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Iterator resultsIter = results.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    final Object next1 = resultsIter.next();
    Object expectedNext1 = expectedResultsIter.next();
    Assert.assertEquals("order-limit", expectedNext1, next1);

    final Object next2 = resultsIter.next();
    Object expectedNext2 = expectedResultsIter.next();
    Assert.assertNotEquals("order-limit", expectedNext2, next2);
  }

  @Test
  public void testPostAggMergedHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("index", "index")
            )
        )
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1, responseContext), runner.run(query2, responseContext));
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testGroupByWithOrderLimitHavingSpec()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-01-25/2011-01-28")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("index", "index")
            )
        )
        .setGranularity(QueryGranularity.ALL)
        .setHavingSpec(new GreaterThanHavingSpec("index", 310L))
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "index",
                        OrderByColumnSpec.Direction.ASCENDING
                    )
                ),
                5
            )
        );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            getDimMapping("business"),
            "rows",
            3L,
            "index",
            312.38165283203125
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            getDimMapping("news"),
            "rows",
            3L,
            "index",
            312.7834167480469
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            getDimMapping("technology"),
            "rows",
            3L,
            "index",
            324.6412353515625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            getDimMapping("travel"),
            "rows",
            3L,
            "index",
            393.36322021484375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            getDimMapping("health"),
            "rows",
            3L,
            "index",
            511.2996826171875
        )
    );

    GroupByQuery fullQuery = builder.build();
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery);
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        ""
    );
  }

  @Test
  public void testPostAggHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("index", "index")
            )
        )
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.<HavingSpec>of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        ""
    );
  }


  @Test
  public void testHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L)
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        ""
    );
  }

  @Test
  public void testMergedHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 6L, "idx", 4416L)
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(runner.run(query1, responseContext), runner.run(query2, responseContext));
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(fullQuery, context), "merged");
  }

  @Test
  public void testMergedPostAggHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("business"),
            "rows",
            2L,
            "idx",
            217L,
            "rows_times_10",
            20.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            6L,
            "idx",
            4420L,
            "rows_times_10",
            60.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            6L,
            "idx",
            4416L,
            "rows_times_10",
            60.0
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "rows_times_10",
                    "*",
                    Arrays.<PostAggregator>asList(
                        new FieldAccessPostAggregator(
                            "rows",
                            "rows"
                        ),
                        new ConstantPostAggregator(
                            "const",
                            10L
                        )
                    )
                )
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows_times_10", 20L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(
              Query<Row> query, Map<String, Object> responseContext
          )
          {
            // simulate two daily segments
            final Query query1 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-02/2011-04-03")))
            );
            final Query query2 = query.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Lists.newArrayList(new Interval("2011-04-03/2011-04-04")))
            );
            return Sequences.concat(
                runner.run(query1, responseContext),
                runner.run(query2, responseContext)
            );
          }
        }
    );

    Map<String, Object> context = Maps.newHashMap();
    // add an extra layer of merging, simulate broker forwarding query to historical
    TestHelper.assertExpectedObjects(
        expectedResults,
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                factory.getToolchest().preMergeQueryDecoration(mergedRunner)
            )
        ).run(fullQuery, context),
        "merged"
    );
  }

  @Test
  public void testGroupByWithRegEx() throws Exception
  {
    String regexPattern = groupingKeyType == ValueType.STRING ? "auto.*" : "11.*";
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter(qualityName, regexPattern))
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, qualityName)))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", qualityName, getDimMapping("automotive"), "rows", 2L)
    );

    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine,
        TestQueryRunners.pool,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    ).mergeResults(runner);
    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");
  }

  @Test
  public void testGroupByWithMetricColumnDisappears() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension(qualityName)
        .addDimension("index")
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "index",
            null,
            qualityName,
            getDimMapping("automotive"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("business"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "index",
            null,
            qualityName,
            getDimMapping("entertainment"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("health"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("mezzanine"), "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("news"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("premium"), "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "index",
            null,
            qualityName,
            getDimMapping("technology"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "index", null, qualityName, getDimMapping("travel"), "rows", 2L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "normal");
    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine,
        TestQueryRunners.pool,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    ).mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");
  }

  @Test
  public void testGroupByWithNonexistentDimension() throws Exception
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension(qualityName)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            qualityName,
            getDimMapping("automotive"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("business"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            qualityName,
            getDimMapping("entertainment"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("health"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("mezzanine"), "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("news"), "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("premium"), "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            qualityName,
            getDimMapping("technology"),
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, qualityName, getDimMapping("travel"), "rows", 2L)
    );

    Map<String, Object> context = Maps.newHashMap();
    TestHelper.assertExpectedObjects(expectedResults, runner.run(query, context), "normal");
    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    QueryRunner<Row> mergeRunner = new GroupByQueryQueryToolChest(
        configSupplier,
        new DefaultObjectMapper(),
        engine,
        TestQueryRunners.pool,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    ).mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(query, context), "no-limit");
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setDimFilter(new JavaScriptDimFilter(qualityName, "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQuery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setDimFilter(new JavaScriptDimFilter(qualityName, "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    new Interval("2011-04-01T00:00:00.000Z/2011-04-01T23:58:00.000Z"),
                    new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        )
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx", 2900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentGroupingSubqueryMultipleAggregatorsOnSameField()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setPostAggregatorSpecs(
            Lists.<PostAggregator>newArrayList(
                new ArithmeticPostAggregator(
                    "post_agg",
                    "+",
                    Lists.<PostAggregator>newArrayList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new FieldAccessPostAggregator("idx", "idx")
                    )
                )
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx1", "idx"),
                new DoubleMaxAggregatorFactory("idx2", "idx"),
                new DoubleMaxAggregatorFactory("idx3", "post_agg"),
                new DoubleMaxAggregatorFactory("idx4", "post_agg")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx1", 2900.0, "idx2", 2900.0,
                                                       "idx3", 5800.0, "idx4", 5800.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx1", 2505.0, "idx2", 2505.0,
                                                       "idx3", 5010.0, "idx4", 5010.0
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, qualityName)))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setDimFilter(
            new OrDimFilter(
                Lists.<DimFilter>newArrayList(
                    new SelectorDimFilter(qualityName, getDimMapping("automotive").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("premium").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("mezzanine").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("business").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("entertainment").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("health").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("news").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("technology").toString()),
                    new SelectorDimFilter(qualityName, getDimMapping("travel").toString())
                )
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx", 2900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.secondOnly)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testEmptySubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new DoubleMaxAggregatorFactory("idx", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setDimFilter(new JavaScriptDimFilter("quality", "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            3L,
            "idx_post",
            13870.0,
            "idx",
            3870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            3L,
            "idx_post",
            13900.0,
            "idx",
            3900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("premium"),
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setDimFilter(new JavaScriptDimFilter(qualityName, "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx_subagg", "index")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getFloatMetric("idx_subpostagg") < 3800);
              }

              @Override
              public byte[] getCacheKey()
              {
                return new byte[0];
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("automotive"),
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("business"),
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("premium"),
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setDimFilter(new JavaScriptDimFilter(marketName, "function(dim){ return true; }"))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new DoubleSumAggregatorFactory("idx_subagg", "index"),
                new JavaScriptAggregatorFactory(
                    "js_agg",
                    Arrays.asList("index", "market"),
                    "function(current, index, dim){return current + index + dim.length;}",
                    "function(){return 0;}",
                    "function(a,b){return a + b;}"
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getFloatMetric("idx_subpostagg") < 3800);
              }

              @Override
              public byte[] getCacheKey()
              {
                return new byte[0];
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx_subpostagg"),
                new DoubleSumAggregatorFactory("js_outer_agg", "js_agg")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec(
                        "alias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("travel"),
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L,
            "js_outer_agg",
            123.92274475097656
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("technology"),
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L,
            "js_outer_agg",
            82.62254333496094
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("news"),
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L,
            "js_outer_agg",
            125.58358001708984
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("health"),
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L,
            "js_outer_agg",
            124.13470458984375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L,
            "js_outer_agg",
            162.74722290039062
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("alias", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx"),
                new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("automotive"),
            "rows",
            2L,
            "idx",
            282L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("business"),
            "rows",
            2L,
            "idx",
            230L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("entertainment"),
            "rows",
            2L,
            "idx",
            324L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("health"),
            "rows",
            2L,
            "idx",
            233L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("mezzanine"),
            "rows",
            6L,
            "idx",
            5317L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("news"),
            "rows",
            2L,
            "idx",
            235L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("premium"),
            "rows",
            6L,
            "idx",
            5405L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("technology"),
            "rows",
            2L,
            "idx",
            175L,
            "uniq",
            1.0002442201269182
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            getDimMapping("travel"),
            "rows",
            2L,
            "idx",
            245L,
            "uniq",
            1.0002442201269182
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSubqueryWithHyperUniquesPostAggregator()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList())
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new HyperUniquesAggregatorFactory("quality_uniques_inner", "quality_uniques")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("quality_uniques_inner_post", "quality_uniques_inner")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList())
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("rows", "rows"),
                new LongSumAggregatorFactory("idx", "idx"),
                new HyperUniquesAggregatorFactory("quality_uniques_outer", "quality_uniques_inner_post")
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques_outer_post", "quality_uniques_outer")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "idx",
            12446L,
            "quality_uniques_outer",
            9.019833517963864,
            "quality_uniques_outer_post",
            9.019833517963864
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithTimeColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                QueryRunnerTestHelper.__timeLongSum
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "ntimestamps",
            13.0,
            "sumtime",
            33843139200000L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByTimeExtraction()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(
            Lists.newArrayList(
                new DefaultDimensionSpec(marketName, marketName),
                new ExtractionDimensionSpec(
                    Column.TIME_COLUMN_NAME,
                    "dayOfWeek",
                    new TimeFormatExtractionFn("EEEE", null, null),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimFilter(
            new OrDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter(marketName, getDimMapping("spot").toString()),
                    new SelectorDimFilter(marketName, getDimMapping("upfront").toString())
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            marketName,
            getDimMapping("spot"),
            "index",
            13219.574157714844,
            "rows",
            117L,
            "addRowsIndexConstant",
            13337.574157714844
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            marketName,
            getDimMapping("spot"),
            "index",
            13557.738830566406,
            "rows",
            117L,
            "addRowsIndexConstant",
            13675.738830566406
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            marketName,
            getDimMapping("spot"),
            "index",
            13493.751281738281,
            "rows",
            117L,
            "addRowsIndexConstant",
            13611.751281738281
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            marketName,
            getDimMapping("spot"),
            "index",
            13585.541015625,
            "rows",
            117L,
            "addRowsIndexConstant",
            13703.541015625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            marketName,
            getDimMapping("spot"),
            "index",
            14279.127197265625,
            "rows",
            126L,
            "addRowsIndexConstant",
            14406.127197265625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            marketName,
            getDimMapping("spot"),
            "index",
            13199.471435546875,
            "rows",
            117L,
            "addRowsIndexConstant",
            13317.471435546875
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Wednesday",
            marketName,
            getDimMapping("spot"),
            "index",
            14271.368591308594,
            "rows",
            126L,
            "addRowsIndexConstant",
            14398.368591308594
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            marketName,
            getDimMapping("upfront"),
            "index",
            27297.8623046875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27324.8623046875
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            marketName,
            getDimMapping("upfront"),
            "index",
            27619.58447265625,
            "rows",
            26L,
            "addRowsIndexConstant",
            27646.58447265625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            marketName,
            getDimMapping("upfront"),
            "index",
            27820.83154296875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27847.83154296875
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            marketName,
            getDimMapping("upfront"),
            "index",
            24791.223876953125,
            "rows",
            26L,
            "addRowsIndexConstant",
            24818.223876953125
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            marketName,
            getDimMapping("upfront"),
            "index",
            28562.748901367188,
            "rows",
            28L,
            "addRowsIndexConstant",
            28591.748901367188
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            marketName,
            getDimMapping("upfront"),
            "index",
            26968.280639648438,
            "rows",
            26L,
            "addRowsIndexConstant",
            26995.280639648438
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01",
            "dayOfWeek",
            "Wednesday",
            marketName,
            getDimMapping("upfront"),
            "index",
            28985.5751953125,
            "rows",
            28L,
            "addRowsIndexConstant",
            29014.5751953125
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testBySegmentResults()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    getDimMapping("mezzanine"),
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(qualityName, "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter(qualityName, getDimMapping("mezzanine").toString()))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResultsUnOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(
                        new MapLookupExtractor(
                            ImmutableMap.of(
                                getDimMapping("mezzanine").toString(),
                                "mezzanine0"
                            )
                        ), false, null, false,
                        false
                    ),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter(qualityName, getDimMapping("mezzanine").toString()))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResultsOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<BySegmentResultValue>(
        new DateTime("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass(
            Arrays.asList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ), "testSegment", new Interval("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    qualityName,
                    "alias",
                    new LookupExtractionFn(
                        new MapLookupExtractor(
                            ImmutableMap.of(
                                getDimMapping("mezzanine").toString(),
                                "mezzanine0"
                            )
                        ), false, null, true,
                        false
                    ),
                    null
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter(qualityName, getDimMapping("mezzanine").toString()))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = Lists.newArrayList();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(bySegmentResults, theRunner.run(fullQuery, Maps.newHashMap()), "");
    exec.shutdownNow();
  }

  // Extraction Filters testing

  @Test
  public void testGroupByWithExtractionDimFilter()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put(getDimMapping("automotive").toString(), "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put(getDimMapping("business").toString(), "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put(getDimMapping("mezzanine").toString(), "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put(getDimMapping("news").toString(), "automotiveAndBusinessAndNewsAndMezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    List<DimFilter> dimFilters = Lists.<DimFilter>newArrayList(
        new ExtractionDimFilter(qualityName, "automotiveAndBusinessAndNewsAndMezzanine", lookupExtractionFn, null),
        new SelectorDimFilter(qualityName, getDimMapping("entertainment").toString()),
        new SelectorDimFilter(qualityName, getDimMapping("health").toString()),
        new SelectorDimFilter(qualityName, getDimMapping("premium").toString()),
        new SelectorDimFilter(qualityName, getDimMapping("technology").toString()),
        new SelectorDimFilter(qualityName, getDimMapping("travel").toString())
    );

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.<DimensionSpec>newArrayList(
                                             new DefaultDimensionSpec(
                                                 qualityName,
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         )
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(Druids.newOrDimFilterBuilder().fields(dimFilters).build())
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

  }

  @Test
  public void testGroupByWithExtractionDimFilterCaseMappingValueIsNullOrEmpty()
  {
    Map<String, String> extractionMap = Maps.newHashMap(QUALITY_RENAMING_MAP);
    extractionMap.put(getDimMapping("mezzanine").toString(), null);
    extractionMap.put(getDimMapping("news").toString(), "");


    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.<DimensionSpec>newArrayList(
                                             new DefaultDimensionSpec(
                                                 qualityName,
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         )
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(new ExtractionDimFilter(qualityName, "", lookupExtractionFn, null))
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithExtractionDimFilterWhenSearchValueNotInTheMap()
  {
    Map<String, String> extractionMap = new HashMap<>();
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.<DimensionSpec>newArrayList(
                                             new DefaultDimensionSpec(
                                                 qualityName,
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         )
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(
                                         new ExtractionDimFilter(
                                             qualityName,
                                             "NOT_THERE",
                                             lookupExtractionFn,
                                             null
                                         )
                                     ).build();
    List<Row> expectedResults = Arrays.asList();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithExtractionDimFilterKeyisNull()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "NULLorEMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.<DimensionSpec>newArrayList(
                                             new DefaultDimensionSpec(
                                                 "null_column",
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         )
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(
                                         new ExtractionDimFilter(
                                             "null_column",
                                             "NULLorEMPTY",
                                             lookupExtractionFn,
                                             null
                                         )
                                     ).build();
    List<Row> expectedResults = Arrays
        .asList(
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWithAggregatorFilterAndExtractionFunction()
  {
    Map<String, String> extractionMap = Maps.newHashMap(QUALITY_RENAMING_MAP);
    extractionMap.put(getDimMapping("mezzanine").toString(), "mezzanineANDnews");
    extractionMap.put(getDimMapping("news").toString(), "mezzanineANDnews");


    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "missing", true, false);
    DimFilter filter = new ExtractionDimFilter(qualityName, "mezzanineANDnews", lookupExtractionFn, null);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(
                                         Lists.<DimensionSpec>newArrayList(
                                             new DefaultDimensionSpec(
                                                 qualityName,
                                                 "alias"
                                             )
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             new FilteredAggregatorFactory(QueryRunnerTestHelper.rowsCount, filter),
                                             (AggregatorFactory) new FilteredAggregatorFactory(
                                                 new LongSumAggregatorFactory(
                                                     "idx",
                                                     "index"
                                                 ), filter
                                             )
                                         )
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("automotive"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("business"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("entertainment"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("health"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("premium"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("technology"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("travel"), "rows", 0L, "idx", 0L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("automotive"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("business"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("entertainment"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("health"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("premium"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("technology"), "rows", 0L, "idx", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("travel"), "rows", 0L, "idx", 0L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

  }

  @Test
  public void testGroupByWithExtractionDimFilterOptimazitionManyToOne()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put(getDimMapping("mezzanine").toString(), "newsANDmezzanine");
    extractionMap.put(getDimMapping("news").toString(), "newsANDmezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(
                                         qualityName,
                                         "alias"
                                     )))
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         ))
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(new ExtractionDimFilter(
                                         qualityName,
                                         "newsANDmezzanine",
                                         lookupExtractionFn,
                                         null
                                     ))
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", getDimMapping("news"), "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("mezzanine"), "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", getDimMapping("news"), "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testGroupByWithExtractionDimFilterNullDims()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);

    GroupByQuery query = GroupByQuery.builder().setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec(
                                         "null_column",
                                         "alias"
                                     )))
                                     .setAggregatorSpecs(
                                         Arrays.asList(
                                             QueryRunnerTestHelper.rowsCount,
                                             new LongSumAggregatorFactory("idx", "index")
                                         ))
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(new ExtractionDimFilter(
                                         "null_column",
                                         "EMPTY",
                                         lookupExtractionFn,
                                         null
                                     )).build();
    List<Row> expectedResults = Arrays
        .asList(
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
