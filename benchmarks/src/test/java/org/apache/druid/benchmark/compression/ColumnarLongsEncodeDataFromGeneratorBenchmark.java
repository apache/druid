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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.generator.ColumnValueGenerator;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
public class ColumnarLongsEncodeDataFromGeneratorBenchmark extends BaseColumnarLongsFromGeneratorBenchmark
{
  @Setup
  public void setup() throws Exception
  {
    vals = new long[rows];
    final String filename = getGeneratorValueFilename(distribution, rows, zeroProbability);
    File dir = getTmpDir();
    File dataFile = new File(dir, filename);

    if (dataFile.exists()) {
      System.out.println("Data files already exist, re-using");
      try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
        int lineNum = 0;
        String line;
        while ((line = br.readLine()) != null) {
          vals[lineNum] = Long.parseLong(line);
          if (vals[lineNum] < minValue) {
            minValue = vals[lineNum];
          }
          if (vals[lineNum] > maxValue) {
            maxValue = vals[lineNum];
          }
          lineNum++;
        }
      }
    } else {
      try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
        ColumnValueGenerator valueGenerator = makeGenerator(distribution, rows, zeroProbability);

        for (int i = 0; i < rows; i++) {
          long value;
          Object rowValue = valueGenerator.generateRowValue();
          value = rowValue != null ? (long) rowValue : 0;
          vals[i] = value;
          if (vals[i] < minValue) {
            minValue = vals[i];
          }
          if (vals[i] > maxValue) {
            maxValue = vals[i];
          }
          writer.write(vals[i] + "\n");
        }
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void encodeColumn(Blackhole blackhole) throws IOException
  {
    File dir = getTmpDir();
    File columnDataFile = new File(dir, getGeneratorEncodedFilename(encoding, distribution, rows, zeroProbability));
    columnDataFile.delete();
    FileChannel output =
        FileChannel.open(columnDataFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

    int size = encodeToFile(vals, encoding, output);
    EncodingSizeProfiler.encodedSize = size;
    blackhole.consume(size);
    output.close();
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ColumnarLongsEncodeDataFromGeneratorBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-longs-encode-speed.csv")
        .build();

    new Runner(opt).run();
  }

  private static String getGeneratorValueFilename(String distribution, int rows, double nullProbability)
  {
    return StringUtils.format("values-%s-%s-%s.bin", distribution, rows, nullProbability);
  }
}
