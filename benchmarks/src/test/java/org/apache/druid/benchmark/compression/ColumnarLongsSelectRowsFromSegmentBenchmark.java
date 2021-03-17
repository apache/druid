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

import com.google.common.io.Files;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class ColumnarLongsSelectRowsFromSegmentBenchmark extends BaseColumnarLongsFromSegmentsBenchmark
{
  private Map<String, ColumnarLongs> decoders;
  private Map<String, Integer> encodedSize;

  /**
   * Number of rows to read, the test will randomly set positions in a simulated offset of the specified density in
   * {@link #setupFilters(int, double)}
   */
  @Param({"0.01", "0.1", "0.33", "0.66", "0.95", "1.0"})
  private double filteredRowCountPercentage;

  @Setup
  public void setup() throws Exception
  {
    decoders = new HashMap<>();
    encodedSize = new HashMap<>();
    setupFilters(rows, filteredRowCountPercentage);

    setupFromFile(encoding);


    // uncomment this block to run sanity check to ensure all specified encodings produce the same set of results
    //CHECKSTYLE.OFF: Regexp
//    List<String> all = ImmutableList.of("lz4-longs", "lz4-auto");
//    for (String _enc : all) {
//      if (!_enc.equals(encoding)) {
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
    File compFile = new File(dir, getColumnEncodedFileName(encoding, segmentName, columnName));
    ByteBuffer buffer = Files.map(compFile);

    int size = (int) compFile.length();
    encodedSize.put(encoding, size);
    ColumnarLongs data = BaseColumnarLongsBenchmark.createColumnarLongs(encoding, buffer);
    decoders.put(encoding, data);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRows(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    ColumnarLongs encoder = decoders.get(encoding);
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

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRowsVectorized(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    ColumnarLongs columnDecoder = decoders.get(encoding);
    long[] vector = new long[VECTOR_SIZE];
    while (!vectorOffset.isDone()) {
      if (vectorOffset.isContiguous()) {
        columnDecoder.get(vector, vectorOffset.getStartOffset(), vectorOffset.getCurrentVectorSize());
      } else {
        columnDecoder.get(vector, vectorOffset.getOffsets(), vectorOffset.getCurrentVectorSize());
      }
      for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++) {
        blackhole.consume(vector[i]);
      }
      vectorOffset.advance();
    }
    blackhole.consume(vector);
    blackhole.consume(vectorOffset);
    vectorOffset.reset();
    columnDecoder.close();
  }


  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ColumnarLongsSelectRowsFromSegmentBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-longs-select-speed-segments.csv")
        .build();

    new Runner(opt).run();
  }
}
