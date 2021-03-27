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

import com.google.common.collect.Iterables;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.LongsColumn;
import org.apache.druid.segment.column.ValueType;
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class ColumnarLongsEncodeDataFromSegmentBenchmark extends BaseColumnarLongsFromSegmentsBenchmark
{
  @Setup
  public void setup() throws Exception
  {
    initializeSegmentValueIntermediaryFile();
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    List<Long> values = new ArrayList<>();
    try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        long value = Long.parseLong(line);
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
        values.add(value);
        rows++;
      }
    }

    vals = values.stream().mapToLong(i -> i).toArray();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void encodeColumn(Blackhole blackhole) throws IOException
  {
    File dir = getTmpDir();
    File columnDataFile = new File(dir, getColumnEncodedFileName(encoding, segmentName, columnName));
    columnDataFile.delete();
    FileChannel output =
        FileChannel.open(columnDataFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

    int size = BaseColumnarLongsBenchmark.encodeToFile(vals, encoding, output);
    EncodingSizeProfiler.encodedSize = size;
    blackhole.consume(size);
    output.close();
  }

  /**
   * writes column values to an intermediary text file, 1 per line, encoders read from this file as input to write
   * encoded column files.
   */
  private void initializeSegmentValueIntermediaryFile() throws IOException
  {
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    if (!dataFile.exists()) {
      final IndexIO indexIO = new IndexIO(
          new DefaultObjectMapper(),
          () -> 0
      );
      try (final QueryableIndex index = indexIO.loadIndex(new File(segmentPath))) {
        final Set<String> columnNames = new LinkedHashSet<>();
        columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
        Iterables.addAll(columnNames, index.getColumnNames());
        final ColumnHolder column = index.getColumnHolder(columnName);
        final ColumnCapabilities capabilities = column.getCapabilities();
        final ValueType columnType = capabilities.getType();
        try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
          if (columnType != ValueType.LONG) {
            throw new RuntimeException("Invalid column type, expected 'Long'");
          }
          LongsColumn theColumn = (LongsColumn) column.getColumn();


          for (int i = 0; i < theColumn.length(); i++) {
            long value = theColumn.getLongSingleValueRow(i);
            writer.write(value + "\n");
          }
        }
      }
    }
  }

  private String getColumnDataFileName(String segmentName, String columnName)
  {
    return StringUtils.format("%s-longs-%s.txt", segmentName, columnName);
  }

  public static void main(String[] args) throws RunnerException
  {
    System.out.println("main happened");
    Options opt = new OptionsBuilder()
        .include(ColumnarLongsEncodeDataFromSegmentBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-longs-encode-speed-segments.csv")
        .build();

    new Runner(opt).run();
  }
}
