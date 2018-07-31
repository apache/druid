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

package io.druid.benchmark.query.timecompare;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.benchmark.query.QueryBenchmarkUtil;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogHash;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.ExprMacroTable;
import io.druid.offheap.OffheapBufferGenerator;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.PerSegmentOptimizingQueryRunner;
import io.druid.query.PerSegmentQueryOptimizationContext;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.filter.IntervalDimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 50)
@Measurement(iterations = 200)
public class TimeCompareBenchmark
{
  @Param({"10"})
  private int numSegments;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"100"})
  private int threshold;

  protected static final Map<String, String> scriptDoubleSum = Maps.newHashMap();
  static {
    scriptDoubleSum.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    scriptDoubleSum.put("fnReset", "function reset() { return 0 }");
    scriptDoubleSum.put("fnCombine", "function combine(a,b) { return a + b }");
  }

  private static final Logger log = new Logger(TimeCompareBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<IncrementalIndex> incIndexes;
  private List<QueryableIndex> qIndexes;

  private QueryRunnerFactory topNFactory;
  private Query topNQuery;
  private QueryRunner topNRunner;


  private QueryRunnerFactory timeseriesFactory;
  private Query timeseriesQuery;
  private QueryRunner timeseriesRunner;

  private BenchmarkSchemaInfo schemaInfo;
  private File tmpDir;
  private Interval[] segmentIntervals;

  private ExecutorService executorService;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
    JSON_MAPPER.setInjectableValues(injectableValues);

    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private static final Map<String, Map<String, Object>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, Object> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

    long startMillis = basicSchema.getDataInterval().getStartMillis();
    long endMillis = basicSchema.getDataInterval().getEndMillis();
    long half = (endMillis - startMillis) / 2;

    Interval recent = Intervals.utc(half, endMillis);
    Interval previous = Intervals.utc(startMillis, half);

    log.info("Recent interval: " + recent);
    log.info("Previous interval: " + previous);

    { // basic.topNTimeCompare
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(
          new FilteredAggregatorFactory(
              //jsAgg1,
              new LongSumAggregatorFactory(
                  "sumLongSequential", "sumLongSequential"
              ),
              new IntervalDimFilter(
                  Column.TIME_COLUMN_NAME,
                  Collections.singletonList(recent),
                  null
              )
          )
      );
      queryAggs.add(
          new FilteredAggregatorFactory(
              new LongSumAggregatorFactory(
                  "_cmp_sumLongSequential", "sumLongSequential"
              ),
              new IntervalDimFilter(
                  Column.TIME_COLUMN_NAME,
                  Collections.singletonList(previous),
                  null
              )
          )
      );

      TopNQueryBuilder queryBuilderA = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension("dimUniform")
          .metric("sumLongSequential")
          .intervals(intervalSpec)
          .aggregators(queryAggs)
          .threshold(threshold);

      topNQuery = queryBuilderA.build();
      topNFactory = new TopNQueryRunnerFactory(
          new StupidPool<>(
              "TopNBenchmark-compute-bufferPool",
              new OffheapBufferGenerator("compute", 250000000),
              0,
              Integer.MAX_VALUE
          ),
          new TopNQueryQueryToolChest(new TopNQueryConfig(), QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()),
          QueryBenchmarkUtil.NOOP_QUERYWATCHER
      );

      basicQueries.put("topNTimeCompare", queryBuilderA);
    }
    { // basic.timeseriesTimeCompare
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(
          new FilteredAggregatorFactory(
              new LongSumAggregatorFactory(
                  "sumLongSequential", "sumLongSequential"
              ),
              new IntervalDimFilter(
                  Column.TIME_COLUMN_NAME,
                  Collections.singletonList(recent),
                  null
              )
          )
      );
      queryAggs.add(
          new FilteredAggregatorFactory(
              new LongSumAggregatorFactory(
                  "_cmp_sumLongSequential", "sumLongSequential"
              ),
              new IntervalDimFilter(
                  Column.TIME_COLUMN_NAME,
                  Collections.singletonList(previous),
                  null
              )
          )
      );

      Druids.TimeseriesQueryBuilder timeseriesQueryBuilder = Druids.newTimeseriesQueryBuilder()
                                                                   .dataSource("blah")
                                                                   .granularity(Granularities.ALL)
                                                                   .intervals(intervalSpec)
                                                                   .aggregators(queryAggs)
                                                                   .descending(false);

      timeseriesQuery = timeseriesQueryBuilder.build();
      timeseriesFactory = new TimeseriesQueryRunnerFactory(
          new TimeseriesQueryQueryToolChest(
              QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
          ),
          new TimeseriesQueryEngine(),
          QueryBenchmarkUtil.NOOP_QUERYWATCHER
      );
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }


  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    executorService = Execs.multiThreaded(numSegments, "TopNThreadPool");

    setupQueries();

    String schemaName = "basic";
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);
    segmentIntervals = new Interval[numSegments];

    long startMillis = schemaInfo.getDataInterval().getStartMillis();
    long endMillis = schemaInfo.getDataInterval().getEndMillis();
    long partialIntervalMillis = (endMillis - startMillis) / numSegments;
    for (int i = 0; i < numSegments; i++) {
      long partialEndMillis = startMillis + partialIntervalMillis;
      segmentIntervals[i] = Intervals.utc(startMillis, partialEndMillis);
      log.info("Segment [%d] with interval [%s]", i, segmentIntervals[i]);
      startMillis = partialEndMillis;
    }

    incIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      log.info("Generating rows for segment " + i);

      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          segmentIntervals[i],
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }
      incIndexes.add(incIndex);
    }

    tmpDir = Files.createTempDir();
    log.info("Using temp dir: " + tmpDir.getAbsolutePath());

    qIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      File indexFile = INDEX_MERGER_V9.persist(
          incIndexes.get(i),
          tmpDir,
          new IndexSpec(),
          null
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      qIndexes.add(qIndex);
    }

    List<QueryRunner<Result<TopNResultValue>>> singleSegmentRunners = Lists.newArrayList();
    QueryToolChest toolChest = topNFactory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      String segmentName = "qIndex" + i;
      QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          topNFactory,
          segmentName,
          new QueryableIndexSegment(segmentName, qIndexes.get(i))
      );
      singleSegmentRunners.add(
          new PerSegmentOptimizingQueryRunner<>(
              toolChest.preMergeQueryDecoration(runner),
              new PerSegmentQueryOptimizationContext(
                  new SegmentDescriptor(segmentIntervals[i], "1", 0)
              )
          )
      );
    }

    topNRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(topNFactory.mergeRunners(executorService, singleSegmentRunners)),
            toolChest
        )
    );

    List<QueryRunner<Result<TimeseriesResultValue>>> singleSegmentRunnersT = Lists.newArrayList();
    QueryToolChest toolChestT = timeseriesFactory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      String segmentName = "qIndex" + i;
      QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          timeseriesFactory,
          segmentName,
          new QueryableIndexSegment(segmentName, qIndexes.get(i))
      );
      singleSegmentRunnersT.add(
          new PerSegmentOptimizingQueryRunner<>(
              toolChestT.preMergeQueryDecoration(runner),
              new PerSegmentQueryOptimizationContext(
                  new SegmentDescriptor(segmentIntervals[i], "1", 0)
              )
          )
      );
    }

    timeseriesRunner = toolChestT.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChestT.mergeResults(timeseriesFactory.mergeRunners(executorService, singleSegmentRunnersT)),
            toolChestT
        )
    );
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexTopN(Blackhole blackhole)
  {
    Sequence<Result<TopNResultValue>> queryResult = topNRunner.run(
        QueryPlus.wrap(topNQuery),
        Maps.<String, Object>newHashMap()
    );
    List<Result<TopNResultValue>> results = queryResult.toList();

    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexTimeseries(Blackhole blackhole)
  {
    Sequence<Result<TimeseriesResultValue>> queryResult = timeseriesRunner.run(
        QueryPlus.wrap(timeseriesQuery),
        Maps.<String, Object>newHashMap()
    );
    List<Result<TimeseriesResultValue>> results = queryResult.toList();

    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}
