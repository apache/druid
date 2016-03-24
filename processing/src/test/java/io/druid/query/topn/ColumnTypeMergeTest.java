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

package io.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class ColumnTypeMergeTest
{
  private String dimName;
  private String dimName2;
  private IncrementalIndex stringIndex;
  private IncrementalIndex numericIndex;

  public ColumnTypeMergeTest() throws Exception
  {
    init();
  }

  private void init() throws Exception
  {
    dimName = "foo";
    dimName2 = "bar";
    List<DimensionSchema> stringDimSchemas = Arrays.asList((DimensionSchema) new StringDimensionSchema(dimName), new StringDimensionSchema(dimName2));
    List<DimensionSchema> numericDimSchemas = Arrays.asList((DimensionSchema) new FloatDimensionSchema(dimName), new LongDimensionSchema(dimName2));

    List<String> floatStrValues = Arrays.asList("1.0", "2.0", "3.0", "4.0");
    List<Float> floatValues = Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f);
    List<String> longStrValues = Arrays.asList("1", "2", "3", "4-abc");
    List<Long> longValues = Arrays.asList(1L, 2L, 3L, 4L);
    IncrementalIndexSchema stringIndexSchema = new IncrementalIndexSchema(
        0L,
        QueryGranularity.NONE,
        new DimensionsSpec(stringDimSchemas, null, null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );

    IncrementalIndexSchema numericIndexSchema = new IncrementalIndexSchema(
        0L,
        QueryGranularity.NONE,
        new DimensionsSpec(numericDimSchemas, null, null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );

    stringIndex = new OnheapIncrementalIndex(stringIndexSchema, true, 1000);
    numericIndex = new OnheapIncrementalIndex(numericIndexSchema, true, 1000);

    for (int i = 0; i < floatStrValues.size(); i++) {
      MapBasedInputRow row = new MapBasedInputRow(
          1,
          Arrays.asList(dimName, dimName2),
          ImmutableMap.<String, Object>of(dimName, floatStrValues.get(i), dimName2, longStrValues.get(i))
      );
      stringIndex.add(row);
    }

    for (int i = 0; i < floatValues.size(); i++) {
      MapBasedInputRow row = new MapBasedInputRow(
          1,
          Arrays.asList(dimName, dimName2),
          ImmutableMap.<String, Object>of(dimName, floatValues.get(i), dimName2, longValues.get(i))
      );
      numericIndex.add(row);
    }
  }

  @Test
  public void testGroupByMergeWithColumnTypeMismatch() throws Exception
  {

    boolean bySegment = false;

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

    QueryRunner<Row> stringRunner = QueryRunnerTestHelper.makeQueryRunner(factory, "string-segment", new IncrementalIndexSegment(stringIndex, "string-segment"));
    QueryRunner<Row> numericRunner = QueryRunnerTestHelper.makeQueryRunner(factory, "float-segment", new IncrementalIndexSegment(numericIndex, "float-segment"));

    List<QueryRunner<Row>> runners = Arrays.asList(stringRunner, numericRunner);

    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), runners)),
            toolChest
        )
    );

    List<Row> expectedResults = Arrays
        .asList(
            GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "foo", 1.0f, "bar", "1", "rows", 2L),
            GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "foo", 2.0f, "bar", "2", "rows", 2L),
            GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "foo", 3.0f, "bar", "3", "rows", 2L),
            GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "foo", 4.0f, "bar", "4", "rows", 1L),
            GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "foo", 4.0f, "bar", "4-abc", "rows", 1L)
        );

    Map<String, ValueType> typeHints = ImmutableMap.<String, ValueType>of("foo", ValueType.FLOAT);

    Interval allTime = new Interval(0, JodaUtils.MAX_INSTANT);
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource("type-test")
        .setInterval(allTime)
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new DefaultDimensionSpec(dimName, dimName),
                new DefaultDimensionSpec(dimName2, dimName2)
            )
        )
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setContext(ImmutableMap.<String, Object>of("bySegment", bySegment, "typeHints", typeHints));

    final GroupByQuery fullQuery = builder.build();

    HashMap ctx = Maps.newHashMap();
    List results = Sequences.toList(theRunner.run(fullQuery, ctx), Lists.newArrayList());
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }


  @Test
  public void testTopNMergeWithColumnTypeMismatch() throws Exception
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        TestQueryRunners.getPool(),
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    QueryRunner<Row> stringRunner = QueryRunnerTestHelper.makeQueryRunner(factory, "string-segment", new IncrementalIndexSegment(stringIndex, "string-segment"));
    QueryRunner<Row> numericRunner = QueryRunnerTestHelper.makeQueryRunner(factory, "float-segment", new IncrementalIndexSegment(numericIndex, "float-segment"));

    List<QueryRunner<Row>> runners = Arrays.asList(stringRunner, numericRunner);

    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    QueryRunner mergedRunner = factory.mergeRunners(Executors.newCachedThreadPool(), runners);

    final QueryRunner<Result<TopNResultValue>> theRunner = chest.mergeResults(chest.preMergeQueryDecoration(mergedRunner));

    Map<String, ValueType> typeHints = ImmutableMap.<String, ValueType>of("foo", ValueType.FLOAT);

    Interval allTime = new Interval(0, JodaUtils.MAX_INSTANT);
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("type-test")
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(dimName)
        .metric("rows")
        .threshold(10)
        .intervals(Arrays.asList(allTime))
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                new CountAggregatorFactory("rows")
            )
        )
        .context(ImmutableMap.<String, Object>of("typeHints", typeHints))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("1970-01-01T00:00:00.001Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>of(
                        "foo", 1.0f,
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "foo", 2.0f,
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "foo", 3.0f,
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "foo", 4.0f,
                        "rows", 2L
                    )
                )
            )
        )
    );

    HashMap ctx = Maps.newHashMap();
    ArrayList<Result<TopNResultValue>> results = Sequences.<Result<TopNResultValue>, ArrayList>toList(theRunner.run(query, ctx),
                                                                                                      Lists.<Result<TopNResultValue>>newArrayList());
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    TopNQuery query2 = new TopNQueryBuilder()
        .dataSource("type-test")
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(dimName2)
        .metric("rows")
        .threshold(10)
        .intervals(Arrays.asList(allTime))
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                new CountAggregatorFactory("rows")
            )
        )
        .context(ImmutableMap.<String, Object>of("typeHints", typeHints))
        .build();

    List<Result<TopNResultValue>> expectedResults2 = Arrays.asList(
        new Result<>(
            new DateTime("1970-01-01T00:00:00.001Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>of(
                        "bar", "1",
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "bar", "2",
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "bar", "3",
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        "bar", "4",
                        "rows", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        "bar", "4-abc",
                        "rows", 1L
                    )
                )
            )
        )
    );

    ctx = Maps.newHashMap();
    ArrayList<Result<TopNResultValue>> results2 = Sequences.<Result<TopNResultValue>, ArrayList>toList(theRunner.run(query2, ctx),
                                                                                                       Lists.<Result<TopNResultValue>>newArrayList());
    TestHelper.assertExpectedObjects(expectedResults2, results2, "");
  }
}
