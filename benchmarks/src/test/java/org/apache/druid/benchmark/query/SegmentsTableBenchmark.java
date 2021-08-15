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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.schema.SegmentsTableBenchamrkBase;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to perform performance benchmark for the segments table in the system schema, assuming that
 * the metadata of published segments and available segments are fixed.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class SegmentsTableBenchmark extends SegmentsTableBenchamrkBase
{
  @Param({"2021-01-01/P3Y"})
  private String availableSegmentsInterval;

  @Param({"2021-01-02/P3Y"})
  private String publishedSegmentsInterval;

  @Param({"DAY"})
  private String segmentGranularity;

  @Param({
      "10",
      "100",
      "1000",
  })
  private int numSegmentsPerInterval;

  @Param({
      "true",
      "false"
  })
  private boolean forceHashBasedMerge;

  @Param({
      "0",
      "1"
  })
  private String sql;

  @Setup(Level.Trial)
  public void setup()
  {
    final PlannerConfig plannerConfig = new PlannerConfig()
    {
      @Override
      public boolean isMetadataSegmentCacheEnable()
      {
        return true;
      }

      @Override
      public boolean isForceHashBasedMergeForSegmentsTable()
      {
        return forceHashBasedMerge;
      }

      @Override
      public long getMetadataSegmentPollPeriod()
      {
        return 1000;
      }
    };
    setupBenchmark(
        plannerConfig,
        segmentGranularity,
        availableSegmentsInterval,
        publishedSegmentsInterval,
        numSegmentsPerInterval
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException
  {
    tearDownBenchmark();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void segmentsTable(Blackhole blackhole) throws Exception
  {
    String sql = QUERIES.get(Integer.parseInt(this.sql));

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(ImmutableMap.of(), sql)) {
      final PlannerResult plannerResult = planner.plan(sql);
      final Sequence<Object[]> resultSequence = plannerResult.run();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }
}
