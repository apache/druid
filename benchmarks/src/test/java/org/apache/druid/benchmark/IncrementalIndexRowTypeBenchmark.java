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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class IncrementalIndexRowTypeBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private IncrementalIndex incIndex;
  private IncrementalIndex incFloatIndex;
  private IncrementalIndex incStrIndex;
  private static AggregatorFactory[] aggs;
  static final int DIMENSION_COUNT = 8;
  static final int MAX_ROWS = 250000;

  private ArrayList<InputRow> longRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> floatRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> stringRows = new ArrayList<InputRow>();


  static {
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(DIMENSION_COUNT + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < DIMENSION_COUNT; ++i) {
      ingestAggregatorFactories.add(
          new LongSumAggregatorFactory(
              StringUtils.format("sumResult%s", i),
              StringUtils.format("Dim_%s", i)
          )
      );
      ingestAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              StringUtils.format("doubleSumResult%s", i),
              StringUtils.format("Dim_%s", i)
          )
      );
    }
    aggs = ingestAggregatorFactories.toArray(new AggregatorFactory[0]);
  }

  private MapBasedInputRow getLongRow(long timestamp, int dimensionCount)
  {
    Random rng = ThreadLocalRandom.current();
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextLong());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getFloatRow(long timestamp, int dimensionCount)
  {
    Random rng = ThreadLocalRandom.current();
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextFloat());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getStringRow(long timestamp, int dimensionCount)
  {
    Random rng = ThreadLocalRandom.current();
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, String.valueOf(rng.nextLong()));
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(aggs)
        .setDeserializeComplexMetrics(false)
        .setReportParseExceptions(false)
        .setMaxRowCount(MAX_ROWS)
        .buildOnheap();
  }

  @Setup
  public void setup()
  {
    for (int i = 0; i < MAX_ROWS; i++) {
      longRows.add(getLongRow(0, DIMENSION_COUNT));
    }

    for (int i = 0; i < MAX_ROWS; i++) {
      floatRows.add(getFloatRow(0, DIMENSION_COUNT));
    }

    for (int i = 0; i < MAX_ROWS; i++) {
      stringRows.add(getStringRow(0, DIMENSION_COUNT));
    }
  }

  @Setup(Level.Iteration)
  public void setup2()
  {
    incIndex = makeIncIndex();
    incFloatIndex = makeIncIndex();
    incStrIndex = makeIncIndex();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void normalLongs(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < MAX_ROWS; i++) {
      InputRow row = longRows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void normalFloats(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < MAX_ROWS; i++) {
      InputRow row = floatRows.get(i);
      int rv = incFloatIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void normalStrings(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < MAX_ROWS; i++) {
      InputRow row = stringRows.get(i);
      int rv = incStrIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }
}
