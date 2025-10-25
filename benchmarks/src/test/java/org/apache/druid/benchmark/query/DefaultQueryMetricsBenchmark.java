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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.FastIntervalStringFormatter;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DefaultQueryMetricsBenchmark
{

  private GroupByQuery query;
  private ServiceEmitter emitter;

  private DefaultQueryMetrics<Query<?>> baseline;
  private DefaultQueryMetrics<Query<?>> withoutInterval;
  private DefaultQueryMetrics<Query<?>> withCustomIntervalToString;

  @Setup(Level.Trial)
  public void setup()
  {
    ImmutableList<Interval> intervals = ImmutableList.of(
        Intervals.of("2020-01-01/2020-01-08"),
        Intervals.of("2020-01-15/2020-01-22")
    );
    query = GroupByQuery
        .builder()
        .setDataSource("ds")
        .setQuerySegmentSpec(new MultipleIntervalSegmentSpec(intervals))
        .setDimensions(new DefaultDimensionSpec("dim", null))
        .setAggregatorSpecs(ImmutableList.of(new CountAggregatorFactory("cnt")))
        .setGranularity(Granularities.ALL)
        .build();

    emitter = new ServiceEmitter("bench", "localhost", new NoopEmitter());

    baseline = new DefaultQueryMetrics<Query<?>>()
    {
      @Override
      public void interval(Query<?> q)
      {
        setDimension(DruidMetrics.INTERVAL, q.getIntervals().stream().map(Interval::toString).toArray(String[]::new));
      }
    };

    withoutInterval = new DefaultQueryMetrics<Query<?>>()
    {
      @Override
      public void interval(Query<?> q)
      {
        // Emit nothing
      }
    };

    withCustomIntervalToString = new DefaultQueryMetrics<Query<?>>()
    {
      @Override
      public void interval(Query<?> q)
      {
        setDimension(DruidMetrics.INTERVAL, q.getIntervals().stream().map(FastIntervalStringFormatter::format).toArray(String[]::new));
      }
    };
  }

  @Benchmark
  public void baselineMetrics(Blackhole bh)
  {
    baseline.query(query);
    baseline.reportQueryTime(1);
    baseline.emit(emitter);
    bh.consume(baseline);
  }

  @Benchmark
  public void withoutIntervalDimension(Blackhole bh)
  {
    withoutInterval.query(query);
    withoutInterval.reportQueryTime(1);
    withoutInterval.emit(emitter);
    bh.consume(withoutInterval);
  }

  @Benchmark
  public void withCustomIntervalToStringDimension(Blackhole bh)
  {
    withCustomIntervalToString.query(query);
    withCustomIntervalToString.reportQueryTime(1);
    withCustomIntervalToString.emit(emitter);
    bh.consume(withCustomIntervalToString);
  }
}
