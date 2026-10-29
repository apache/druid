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

package org.apache.druid.benchmark.query;

import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Scan queries through an MSQ engine over a datasource split into one-day time chunks, each holding one or more
 * segments that span the whole chunk. Subclasses choose the engine. Rows and the clustered layout
 * come from {@link SqlBenchmarkDatasets#clusteringDatasource}, and {@link #layout} chooses how segments store them.
 * <p>
 * Besides time per query, reports the work done by the scan stage (stage 0) as JMH auxiliary counters, read from the
 * query report: segments acquired, rows in those segments, and rows written, each averaged over queries. Run with
 * {@code -prof gc} to also report bytes allocated per query.
 * <p>
 * Segments default to V9, sorted by {@code __time} first. Choose other layouts with the {@code layout} parameter, for
 * example {@code -p layout=V9_TIME_ORDERED,V10_TIME_ORDERED,V10_CLUSTERED}; see {@link Layout}.
 * <p>
 * To run, build the benchmarks jar and select benchmarks and parameters, passing the JVM flags listed in the "Strong
 * encapsulation" section of docs/operations/java.md:
 * <pre>
 * mvn package -pl benchmarks -am -DskipTests
 * java -jar benchmarks/target/benchmarks.jar 'DartScanBenchmark.timeDescLimit' \
 *   -p numTimeChunks=30 -p limit=10000 -p layout=V10_CLUSTERED -prof gc \
 *   -jvmArgsAppend "--add-opens=java.base/java.nio=ALL-UNNAMED ..."
 * </pre>
 * Set the {@code DRUID_BENCHMARK_CACHE_DIR} environment variable to reuse generated segments across runs.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
public abstract class MSQScanBenchmarkBase extends BaseCalciteQueryTest
{
  static final String DATASOURCE = "scan_ds";
  private static final DateTime FIRST_DAY = DateTimes.of("2026-01-01");

  /**
   * How segments store their rows.
   */
  public enum Layout
  {
    /**
     * V9 segment sorted by {@code __time} first.
     */
    V9_TIME_ORDERED,

    /**
     * V10 segment sorted by {@code __time} first.
     */
    V10_TIME_ORDERED,

    /**
     * V10 clustered base table: a value group per combination of clusterKey1 and clusterKey2, each sorted by
     * {@code __time}.
     */
    V10_CLUSTERED
  }

  /**
   * Segments generated by {@link #setup()}, attached to the walker by the subclass's component supplier. Static
   * because the test framework creates the component supplier itself.
   */
  static final Map<DataSegment, QueryableIndex> SEGMENTS = new LinkedHashMap<>();

  /**
   * Number of one-day time chunks.
   */
  @Param({"1", "30"})
  private int numTimeChunks;

  /**
   * Number of segments per time chunk. Each one spans its whole time chunk.
   */
  @Param({"1", "4"})
  private int numPartitionsPerTimeChunk;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"10", "10000"})
  private int limit;

  @Param({"2"})
  private int maxNumTasks;

  @Param({"V9_TIME_ORDERED"})
  private Layout layout;

  /**
   * Number of distinct values of clusterKey1, the primary clustering column.
   */
  @Param({"100"})
  private int clusteringCardinality;

  private final Closer closer = Closer.create();
  private List<Annotation> annotations;

  /**
   * Mean work done by the scan stage per query. Work can vary between runs of the same query, because concurrent
   * processors that share a limit decide what to read based on each other's progress.
   * <p>
   * JMH sums {@link AuxCounters.Type#EVENTS} counters over measurement iterations, so the counters hold the mean of
   * the first measurement iteration's queries and zero in the others. Their sum is then the mean work of one query.
   */
  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class WorkCounters
  {
    public double segmentsAcquired;
    public double rowsInAcquiredSegments;
    public double rowsWritten;

    private final long[] workSums = new long[3];
    private long numQueries;
    private boolean reported;
    private boolean reporting;

    @Setup(Level.Iteration)
    public void startIteration(final IterationParams iterationParams)
    {
      segmentsAcquired = 0;
      rowsInAcquiredSegments = 0;
      rowsWritten = 0;
      Arrays.fill(workSums, 0);
      numQueries = 0;
      reporting = !reported && iterationParams.getType() == IterationType.MEASUREMENT;
    }

    @TearDown(Level.Iteration)
    public void endIteration()
    {
      reported |= reporting;
    }

    private void record(final long segmentsAcquired, final long rowsInAcquiredSegments, final long rowsWritten)
    {
      if (!reporting) {
        return;
      }

      workSums[0] += segmentsAcquired;
      workSums[1] += rowsInAcquiredSegments;
      workSums[2] += rowsWritten;
      numQueries++;

      // JMH reads the counters when the iteration ends, so keep them at the running mean.
      this.segmentsAcquired = (double) workSums[0] / numQueries;
      this.rowsInAcquiredSegments = (double) workSums[1] / numQueries;
      this.rowsWritten = (double) workSums[2] / numQueries;
    }
  }

  @Setup(Level.Trial)
  public void setup()
  {
    // JMH runs a generated subclass, so find the class that chose the engine.
    Class<?> engineClass = getClass();
    while (!engineClass.isAnnotationPresent(SqlTestFrameworkConfig.ComponentSupplier.class)) {
      engineClass = engineClass.getSuperclass();
    }
    annotations = Arrays.asList(engineClass.getAnnotations());

    final String clusteredDatasource = SqlBenchmarkDatasets.clusteringDatasource("CLUSTERED", clusteringCardinality, 2);
    final SqlBenchmarkDatasets.BenchmarkSchema clustered = SqlBenchmarkDatasets.getSchema(clusteredDatasource);
    final GeneratorSchemaInfo schema = clustered.getGeneratorSchemaInfo();
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec = clustered.getClusterSpec();
    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    SEGMENTS.clear();
    for (int chunk = 0; chunk < numTimeChunks; chunk++) {
      final Interval day = new Interval(FIRST_DAY.plusDays(chunk), FIRST_DAY.plusDays(chunk + 1));
      final GeneratorSchemaInfo daySchema = new GeneratorSchemaInfo(
          schema.getColumnSchemas(),
          schema.getAggs(),
          day,
          schema.isWithRollup()
      );
      for (int partition = 0; partition < numPartitionsPerTimeChunk; partition++) {
        final DataSegment dataSegment = DataSegment.builder()
                                                   .dataSource(DATASOURCE)
                                                   .interval(day)
                                                   .version("1")
                                                   .shardSpec(new LinearShardSpec(partition))
                                                   .size(0)
                                                   .build();
        final QueryableIndex index = switch (layout) {
          case V9_TIME_ORDERED -> segmentGenerator.generate(dataSegment, daySchema, Granularities.NONE, rowsPerSegment);
          case V10_TIME_ORDERED -> segmentGenerator.generateV10(
              dataSegment,
              daySchema,
              daySchema.getDimensionsSpec(),
              TransformSpec.NONE,
              IndexSpec.getDefault(),
              Granularities.NONE,
              null,
              rowsPerSegment,
              CalciteTests.getJsonMapper()
          );
          case V10_CLUSTERED -> segmentGenerator.generateV10(
              dataSegment,
              daySchema,
              clusterSpec.getDimensionsSpec(),
              TransformSpec.NONE,
              IndexSpec.getDefault(),
              Granularities.NONE,
              clusterSpec,
              rowsPerSegment,
              CalciteTests.getJsonMapper()
          );
        };
        SEGMENTS.put(dataSegment, closer.register(index));
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  public void timeDescLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery("SELECT * FROM " + DATASOURCE + " ORDER BY __time DESC LIMIT " + limit, counters, blackhole);
  }

  @Benchmark
  public void timeAscLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery("SELECT * FROM " + DATASOURCE + " ORDER BY __time ASC LIMIT " + limit, counters, blackhole);
  }

  @Benchmark
  public void timeDescThenDimLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery(
        "SELECT * FROM " + DATASOURCE + " ORDER BY __time DESC, dimSecondary LIMIT " + limit,
        counters,
        blackhole
    );
  }

  /**
   * Newest rows for one value of the primary clustering column.
   */
  @Benchmark
  public void clusterFilteredTimeDescLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery(
        "SELECT * FROM " + DATASOURCE + " WHERE clusterKey1 = '1' ORDER BY __time DESC LIMIT " + limit,
        counters,
        blackhole
    );
  }

  @Benchmark
  public void unorderedLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery("SELECT * FROM " + DATASOURCE + " LIMIT " + limit, counters, blackhole);
  }

  @Benchmark
  public void dimOrderLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery("SELECT * FROM " + DATASOURCE + " ORDER BY dimSecondary LIMIT " + limit, counters, blackhole);
  }

  /**
   * Ordered by time without a limit; does not use {@link #limit}. The filter keeps a small fraction of rows.
   */
  @Benchmark
  public void timeDescFilteredNoLimit(final WorkCounters counters, final Blackhole blackhole)
  {
    runQuery(
        "SELECT * FROM " + DATASOURCE + " WHERE dimSecondary = '7' ORDER BY __time DESC",
        counters,
        blackhole
    );
  }

  /**
   * Runs {@code sql} and returns the complete report of the query that ran it.
   */
  protected abstract MSQTaskReportPayload runQueryAndGetReport(
      String sql,
      Map<String, Object> context,
      Blackhole blackhole
  );

  private void runQuery(final String sql, final WorkCounters counters, final Blackhole blackhole)
  {
    queryFrameworkRule.setConfig(new SqlTestFrameworkConfig(annotations));
    final Map<String, Object> context = QueryContext.ofMap(QueryContextParameters.MAX_NUM_TASKS, maxNumTasks);
    final MSQTaskReportPayload payload = runQueryAndGetReport(sql, context, blackhole);

    if (!payload.getStatus().getStatus().isSuccess()) {
      throw new ISE("Query failed: %s", payload.getStatus().getErrorReport());
    }

    // Stage 0 scans the datasource; sum its counters over workers.
    final Map<Integer, CounterSnapshots> scanStageWorkers = payload.getCounters().copyMap().get(0);
    long segmentsAcquired = 0;
    long rowsInAcquiredSegments = 0;
    long rowsWritten = 0;
    for (final CounterSnapshots worker : scanStageWorkers.values()) {
      final ChannelCounters.Snapshot input =
          (ChannelCounters.Snapshot) worker.getMap().get(CounterNames.inputChannel(0));
      final ChannelCounters.Snapshot output =
          (ChannelCounters.Snapshot) worker.getMap().get(CounterNames.outputChannel());
      segmentsAcquired += sum(input.getFiles());
      rowsInAcquiredSegments += sum(input.getRows());
      rowsWritten += output == null ? 0 : sum(output.getRows());
    }
    counters.record(segmentsAcquired, rowsInAcquiredSegments, rowsWritten);
  }

  private static long sum(final long[] values)
  {
    return values == null ? 0 : Arrays.stream(values).sum();
  }

  /**
   * Adds {@link #SEGMENTS} to {@code walker}.
   */
  static SpecificSegmentsQuerySegmentWalker addSegments(final SpecificSegmentsQuerySegmentWalker walker)
  {
    for (final Map.Entry<DataSegment, QueryableIndex> entry : SEGMENTS.entrySet()) {
      walker.add(entry.getKey(), entry.getValue());
    }
    return walker;
  }
}
