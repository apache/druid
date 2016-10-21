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

import com.google.common.base.Function;

import io.druid.java.util.common.parsers.TimestampParser;

import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class TimeParseBenchmark
{
  // 1 million rows
  int numRows = 1000000;

  // Number of batches of same times
  @Param({"10000", "100000", "500000", "1000000"})
  int numBatches;

  static final String DATA_FORMAT = "MM/dd/yyyy HH:mm:ss Z";

  static Function<String, DateTime> timeFn = TimestampParser.createTimestampParser(DATA_FORMAT);

  private String[] rows;

  @Setup
  public void setup()
  {
    SimpleDateFormat format = new SimpleDateFormat(DATA_FORMAT);
    long start = System.currentTimeMillis();
    int rowsPerBatch = numRows / numBatches;
    int numRowInBatch = 0;
    rows = new String[numRows];
    for (int i = 0; i < numRows; ++i) {
      if (numRowInBatch >= rowsPerBatch) {
        numRowInBatch = 0;
        start += 5000; // new batch, add 5 seconds
      }
      rows[i] = format.format(new Date(start));
      numRowInBatch++;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseNoContext(Blackhole blackhole)
  {
    for (int i = 0; i < rows.length; ++i) {
      blackhole.consume(timeFn.apply(rows[i]).getMillis());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void parseWithContext(Blackhole blackhole)
  {
    String lastTimeString = null;
    long lastTime = 0L;
    for (int i = 0; i < rows.length; ++i) {
      if (!rows[i].equals(lastTimeString)) {
        lastTimeString = rows[i];
        lastTime = timeFn.apply(rows[i]).getMillis();
      }
      blackhole.consume(lastTime);
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(TimeParseBenchmark.class.getSimpleName())
        .warmupIterations(1)
        .measurementIterations(10)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
