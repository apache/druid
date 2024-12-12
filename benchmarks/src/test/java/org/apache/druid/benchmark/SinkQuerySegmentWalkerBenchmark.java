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

package org.apache.druid.benchmark;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorTester;
import org.apache.druid.segment.realtime.sink.Committers;
import org.apache.druid.timeline.partition.LinearShardSpec;
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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SinkQuerySegmentWalkerBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"10", "50", "100", "200"})
  private int numFireHydrants;

  private final LoggingEmitter loggingEmitter = new LoggingEmitter(new Logger(LoggingEmitter.class), LoggingEmitter.Level.INFO, new DefaultObjectMapper());
  private final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "test", loggingEmitter);
  private File cacheDir;

  private Appenderator appenderator;

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    final String userConfiguredCacheDir = System.getProperty("druid.benchmark.cacheDir", System.getenv("DRUID_BENCHMARK_CACHE_DIR"));
    cacheDir = new File(userConfiguredCacheDir);
    final StreamAppenderatorTester tester =
        new StreamAppenderatorTester.Builder().maxRowsInMemory(1)
                                              .basePersistDirectory(cacheDir)
                                              .withServiceEmitter(serviceEmitter)
                                              .build();

    appenderator = tester.getAppenderator();
    appenderator.startJob();

    final SegmentIdWithShardSpec segmentIdWithShardSpec = new SegmentIdWithShardSpec(
        StreamAppenderatorTester.DATASOURCE,
        Intervals.of("2000/2001"),
        "A",
        new LinearShardSpec(0)
    );

    for (int i = 0; i < numFireHydrants; i++) {
      final MapBasedInputRow inputRow = new MapBasedInputRow(
          DateTimes.of("2000").getMillis(),
          ImmutableList.of("dim"),
          ImmutableMap.of(
              "dim",
              "bar_" + i,
              "met",
              1
          )
      );
      appenderator.add(segmentIdWithShardSpec, inputRow, Suppliers.ofInstance(Committers.nil()));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    appenderator.close();
    FileUtils.deleteDirectory(cacheDir);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void emitSinkMetrics(Blackhole blackhole) throws Exception
  {
    {
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(StreamAppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001")))
                                           .aggregators(
                                               Arrays.asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results =
          QueryPlus.wrap(query1).run(appenderator, ResponseContext.createEmpty()).toList();
      blackhole.consume(results);

      serviceEmitter.flush();
    }
  }
}
