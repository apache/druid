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

package org.apache.druid.benchmark.compression;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.segment.data.ColumnarLongs;
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
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ColumnarLongsSelectRowsFromGeneratorBenchmark extends BaseColumnarLongsFromGeneratorBenchmark
{
  /**
   * Number of rows to read, the test will randomly set positions in a simulated offset of the specified density in
   * {@link #setupFilters(int, double, String)}
   */
  @Param({
      "0.1",
      "0.25",
      "0.5",
      "0.6",
      "0.75",
      "0.8",
      "0.9",
      "0.95",
      "1.0"
  })
  private double filteredRowCountPercentage;

  @Param({
      "random",
      "contiguous-start",
      "contiguous-end",
      "contiguous-bitmap-start",
      "contiguous-bitmap-end",
      "chunky-1000",
      "chunky-10000"
  })
  private String filterDistribution;

  @Setup
  public void setup() throws IOException
  {
    setupFromFile(encoding);
    setupFilters(rows, filteredRowCountPercentage, filterDistribution);

    // uncomment this block to run sanity check to ensure all specified encodings produce the same set of results
    //CHECKSTYLE.OFF: Regexp
//    ImmutableList<String> all = ImmutableList.of("lz4-longs", "lz4-auto");
//    for (String _enc : all) {
//      if (!_enc.equalsIgnoreCase(encoding)) {
//        setupFromFile(_enc);
//      }
//    }
//
//    checkSanity(decoders, all, rows);
    //CHECKSTYLE.ON: Regexp
  }

  @TearDown
  public void teardown()
  {
    for (ColumnarLongs longs : decoders.values()) {
      longs.close();
    }
  }

  private void setupFromFile(String encoding) throws IOException
  {
    File dir = getTmpDir();
    File compFile = new File(dir, getGeneratorEncodedFilename(encoding, distribution, rows, zeroProbability));
    ByteBuffer buffer = FileUtils.map(compFile).get();

    int size = (int) compFile.length();
    encodedSize.put(encoding, size);
    ColumnarLongs data = createColumnarLongs(encoding, buffer);
    decoders.put(encoding, data);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRows(Blackhole blackhole)
  {
    scan(blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRowsVectorized(Blackhole blackhole)
  {
    scanVectorized(blackhole);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ColumnarLongsSelectRowsFromGeneratorBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-longs-select-speed.csv")
        .build();

    new Runner(opt).run();
  }
}
