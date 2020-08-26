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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.IndexedInts;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ColumnarIntsSelectRowsFromSegmentBenchmark extends BaseColumnarIntsFromSegmentsBenchmark
{
  private Map<String, ColumnarInts> encoders;
  private Map<String, Integer> encodedSize;

  // Number of rows to read, the test will read random rows
  @Param({"0.01", "0.1", "0.33", "0.66", "0.95", "1.0"})
  private double filteredRowCountPercentage;

  Random rand = new Random(0);

  @Setup
  public void setup() throws Exception
  {
    encoders = new HashMap<>();
    encodedSize = new HashMap<>();
    setupFilters(rows, filteredRowCountPercentage);

    setupFromFile(encoding);


    // uncomment me to load some encoding files to cross reference values for sanity check
    //CHECKSTYLE.OFF: Regexp
    ImmutableList<String> all = ImmutableList.of("compressed-vsize-byte", "shapeshift");
    for (String _enc : all) {
      if (!_enc.equals(encoding)) {
        setupFromFile(_enc);
      }
    }
    checkSanity(encoders, all, rows);
    //CHECKSTYLE.ON: Regexp
  }

  @TearDown
  public void teardown() throws Exception
  {
    for (ColumnarInts ints : encoders.values()) {
      ints.close();
    }
  }

  private void setupFromFile(String encoding) throws IOException
  {
    File dir = getTmpDir();
    File compFile = new File(dir, getColumnEncodedFileName(encoding, segmentName, columnName));
    ByteBuffer buffer = Files.map(compFile);

    int size = (int) compFile.length();
    encodedSize.put(encoding, size);
    ColumnarInts data = BaseColumnarIntsBenchmark.createIndexedInts(encoding, buffer, size);
    encoders.put(encoding, data);
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRows(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    IndexedInts encoder = encoders.get(encoding);
    if (filter == null) {
      for (int i = 0; i < rows; i++) {
        blackhole.consume(encoder.get(i));
      }
    } else {
      for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
        blackhole.consume(encoder.get(i));
      }
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ColumnarIntsSelectRowsFromSegmentBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-ints-select-speed-segments.csv")
        .build();

    new Runner(opt).run();
  }
}
