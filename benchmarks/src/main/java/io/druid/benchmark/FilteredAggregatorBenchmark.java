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

package io.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.benchmark.query.QueryBenchmarkUtil;
import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogHash;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.js.JavaScriptConfig;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class FilteredAggregatorBenchmark
{
  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  private static final Logger log = new Logger(FilteredAggregatorBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;
  private IncrementalIndex incIndex;
  private IncrementalIndex incIndexFilteredAgg;
  private AggregatorFactory[] filteredMetrics;
  private QueryableIndex qIndex;
  private File indexFile;
  private DimFilter filter;
  private List<InputRow> inputRows;
  private QueryRunnerFactory factory;
  private BenchmarkSchemaInfo schemaInfo;
  private TimeseriesQuery query;
  private File tmpDir;

  private static String JS_FN = "function(str) { return 'super-' + str; }";
  private static ExtractionFn JS_EXTRACTION_FN = new JavaScriptExtractionFn(
      JS_FN,
      false,
      JavaScriptConfig.getEnabledInstance()
  );

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    incIndex = makeIncIndex(schemaInfo.getAggsArray());

    filter = new OrDimFilter(
        Arrays.asList(
            new BoundDimFilter("dimSequential", "-1", "-1", true, true, null, null, StringComparators.ALPHANUMERIC),
            new JavaScriptDimFilter(
                "dimSequential",
                "function(x) { return false }",
                null,
                JavaScriptConfig.getEnabledInstance()
            ),
            new RegexDimFilter("dimSequential", "X", null),
            new SearchQueryDimFilter("dimSequential", new ContainsSearchQuerySpec("X", false), null),
            new InDimFilter("dimSequential", Collections.singletonList("X"), null)
        )
    );
    filteredMetrics = new AggregatorFactory[1];
    filteredMetrics[0] = new FilteredAggregatorFactory(new CountAggregatorFactory("rows"), filter);
    incIndexFilteredAgg = makeIncIndex(filteredMetrics);

    inputRows = new ArrayList<>();
    for (int j = 0; j < rowsPerSegment; j++) {
      InputRow row = gen.nextRow();
      if (j % 10000 == 0) {
        log.info(j + " rows generated.");
      }
      incIndex.add(row);
      inputRows.add(row);
    }

    tmpDir = Files.createTempDir();
    log.info("Using temp dir: " + tmpDir.getAbsolutePath());

    indexFile = INDEX_MERGER_V9.persist(
        incIndex,
        tmpDir,
        new IndexSpec()
    );
    qIndex = INDEX_IO.loadIndex(indexFile);

    factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(
            QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new TimeseriesQueryEngine(),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );

    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
    List<AggregatorFactory> queryAggs = new ArrayList<>();
    queryAggs.add(filteredMetrics[0]);

    query = Druids.newTimeseriesQueryBuilder()
                  .dataSource("blah")
                  .granularity(Granularities.ALL)
                  .intervals(intervalSpec)
                  .aggregators(queryAggs)
                  .descending(false)
                  .build();
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private IncrementalIndex makeIncIndex(AggregatorFactory[] metrics)
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(metrics)
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }

  // Filtered agg doesn't work with ingestion, cardinality is not supported in incremental index
  // See https://github.com/druid-io/druid/issues/3164
  // @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void ingest(Blackhole blackhole) throws Exception
  {
    incIndexFilteredAgg = makeIncIndex(filteredMetrics);
    for (InputRow row : inputRows) {
      int rv = incIndexFilteredAgg.add(row);
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "incIndex",
        new IncrementalIndexSegment(incIndex, "incIndex")
    );

    List<Result<TimeseriesResultValue>> results = FilteredAggregatorBenchmark.runQuery(factory, runner, query);
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole) throws Exception
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndex)
    );

    List<Result<TimeseriesResultValue>> results = FilteredAggregatorBenchmark.runQuery(factory, runner, query);
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}
