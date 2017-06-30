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

import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class IncrementalIndexRowTypeBenchmark
{
  private IncrementalIndex incIndex;
  private IncrementalIndex incFloatIndex;
  private IncrementalIndex incStrIndex;
  private static AggregatorFactory[] aggs;
  static final int dimensionCount = 8;
  private Random rng;
  static final int maxRows = 250000;

  private ArrayList<InputRow> longRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> floatRows = new ArrayList<InputRow>();
  private ArrayList<InputRow> stringRows = new ArrayList<InputRow>();


  static {
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
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

  private MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextLong());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getFloatRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, rng.nextFloat());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private MapBasedInputRow getStringRow(long timestamp, int rowID, int dimensionCount)
  {
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
        .setMaxRowCount(maxRows)
        .buildOnheap();
  }

  @Setup
  public void setup() throws IOException
  {
    rng = new Random(9999);

    for (int i = 0; i < maxRows; i++) {
      longRows.add(getLongRow(0, i, dimensionCount));
    }

    for (int i = 0; i < maxRows; i++) {
      floatRows.add(getFloatRow(0, i, dimensionCount));
    }

    for (int i = 0; i < maxRows; i++) {
      stringRows.add(getStringRow(0, i, dimensionCount));
    }
  }

  @Setup(Level.Iteration)
  public void setup2() throws IOException
  {
    ;
    incIndex = makeIncIndex();
    incFloatIndex = makeIncIndex();
    incStrIndex = makeIncIndex();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalLongs(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = longRows.get(i);
      int rv = incIndex.add(row);
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalFloats(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = floatRows.get(i);
      int rv = incFloatIndex.add(row);
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(maxRows)
  public void normalStrings(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < maxRows; i++) {
      InputRow row = stringRows.get(i);
      int rv = incStrIndex.add(row);
      blackhole.consume(rv);
    }
  }
}
