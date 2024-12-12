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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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

  @Param({"250000"})
  private int rowsPerSegment;

  @Param({"onheap", "offheap"})
  private String indexType;

  private AppendableIndexSpec appendableIndexSpec;
  IncrementalIndex incIndex;
  private static AggregatorFactory[] aggs;
  static final int DIMENSION_COUNT = 8;

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
    return appendableIndexSpec.builder()
        .setSimpleTestingIndexSchema(aggs)
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  @Setup
  public void setup() throws JsonProcessingException
  {
    appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);

    for (int i = 0; i < rowsPerSegment; i++) {
      longRows.add(getLongRow(0, DIMENSION_COUNT));
    }

    for (int i = 0; i < rowsPerSegment; i++) {
      floatRows.add(getFloatRow(0, DIMENSION_COUNT));
    }

    for (int i = 0; i < rowsPerSegment; i++) {
      stringRows.add(getStringRow(0, DIMENSION_COUNT));
    }
  }

  @Setup(Level.Invocation)
  public void setup2()
  {
    incIndex = makeIncIndex();
  }

  @Setup(Level.Invocation)
  public void tearDown()
  {
    incIndex.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void normalLongs(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = longRows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void normalFloats(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = floatRows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void normalStrings(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = stringRows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }
}
