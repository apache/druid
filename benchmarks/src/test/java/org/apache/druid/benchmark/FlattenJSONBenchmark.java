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

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@Fork(value = 1)
public class FlattenJSONBenchmark
{
  private static final int NUM_EVENTS = 100000;

  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "iso", null),
      DimensionsSpec.EMPTY,
      ColumnsFilter.all()
  );

  List<byte[]> flatInputBytes;
  List<byte[]> nestedInputBytes;
  List<byte[]> jqInputBytes;
  JsonInputFormat flatFormat;
  JsonInputFormat nestedFormat;
  JsonInputFormat jqFormat;
  JsonInputFormat treeJqFormat;
  JsonInputFormat treeTreeFormat;
  JsonInputFormat fieldDiscoveryFormat;
  JsonInputFormat forcedPathFormat;
  int flatCounter = 0;
  int nestedCounter = 0;
  int jqCounter = 0;

  @Setup
  public void prepare() throws Exception
  {
    FlattenJSONBenchmarkUtil gen = new FlattenJSONBenchmarkUtil();
    flatInputBytes = new ArrayList<>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      flatInputBytes.add(StringUtils.toUtf8(gen.generateFlatEvent()));
    }
    nestedInputBytes = new ArrayList<>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      nestedInputBytes.add(StringUtils.toUtf8(gen.generateNestedEvent()));
    }
    jqInputBytes = new ArrayList<>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      jqInputBytes.add(StringUtils.toUtf8(gen.generateNestedEvent())); // reuse the same event as "nested"
    }

    flatFormat = gen.getFlatFormat();
    nestedFormat = gen.getNestedFormat();
    jqFormat = gen.getJqFormat();
    treeJqFormat = gen.getTreeJqFormat();
    treeTreeFormat = gen.getTreeTreeFormat();
    fieldDiscoveryFormat = gen.getFieldDiscoveryFormat();
    forcedPathFormat = gen.getForcedPathFormat();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow baseline(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = flatFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(flatInputBytes.get(flatCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      flatCounter = (flatCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow flatten(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = nestedFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(nestedInputBytes.get(nestedCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow treejqflatten(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = treeJqFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(nestedInputBytes.get(jqCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      jqCounter = (jqCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow treetreeflatten(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = treeTreeFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(nestedInputBytes.get(jqCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      jqCounter = (jqCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow jqflatten(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = jqFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(jqInputBytes.get(jqCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      jqCounter = (jqCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow preflattenNestedParser(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = fieldDiscoveryFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(flatInputBytes.get(nestedCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public InputRow forcedRootPaths(final Blackhole blackhole) throws Exception
  {
    try (CloseableIterator<InputRow> iterator = forcedPathFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(flatInputBytes.get(nestedCounter)),
        null
    ).read()) {
      InputRow row = iterator.next();
      for (String dim : row.getDimensions()) {
        blackhole.consume(row.getRaw(dim));
      }
      nestedCounter = (nestedCounter + 1) % NUM_EVENTS;
      return row;
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(FlattenJSONBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
