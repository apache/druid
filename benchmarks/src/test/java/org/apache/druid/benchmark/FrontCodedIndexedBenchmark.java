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

import com.google.common.base.Preconditions;
import org.apache.druid.benchmark.compression.EncodingSizeProfiler;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.FrontCodedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(GenericIndexedBenchmark.ITERATIONS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class FrontCodedIndexedBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"10000", "100000"})
  public int numElements;

  @Param({"16"})
  public int width;

  @Param({"generic", "front-coded-4", "front-coded-16"})
  public String indexType;

  @Param({"10000"})
  public int numOperations;

  private File fileFrontCoded;
  private File fileGeneric;
  private File smooshDirFrontCoded;
  private File smooshDirGeneric;
  private GenericIndexed<ByteBuffer> genericIndexed;
  private FrontCodedIndexed frontCodedIndexed;

  private Indexed<ByteBuffer> indexed;

  private String[] values;
  private int[] iterationIndexes;
  private String[] elementsToSearch;

  private int written = 0;

  @Setup(Level.Trial)
  public void createIndex() throws IOException
  {
    values = new String[numElements];
    TreeSet<String> set = new TreeSet<>(ColumnType.STRING.getStrategy());
    while (set.size() < numElements) {
      set.add(getRandomId(width));
    }

    Iterator<String> iterator = set.iterator();

    GenericIndexedWriter<String> genericIndexedWriter = new GenericIndexedWriter<>(
        new OffHeapMemorySegmentWriteOutMedium(),
        "genericIndexedBenchmark",
        GenericIndexed.STRING_STRATEGY
    );
    genericIndexedWriter.open();

    FrontCodedIndexedWriter frontCodedIndexedWriter = new FrontCodedIndexedWriter(
        new OnHeapMemorySegmentWriteOutMedium(),
        ByteOrder.nativeOrder(),
        "front-coded-4".equals(indexType) ? 4 : 16
    );
    frontCodedIndexedWriter.open();

    int count = 0;
    while (iterator.hasNext()) {
      final String next = iterator.next();
      values[count++] = next;
      frontCodedIndexedWriter.write(StringUtils.toUtf8Nullable(next));
      genericIndexedWriter.write(next);
    }
    smooshDirFrontCoded = FileUtils.createTempDir();
    fileFrontCoded = File.createTempFile("frontCodedIndexedBenchmark", "meta");
    smooshDirGeneric = FileUtils.createTempDir();
    fileGeneric = File.createTempFile("genericIndexedBenchmark", "meta");

    EncodingSizeProfiler.encodedSize = (int) ("generic".equals(indexType)
                                              ? genericIndexedWriter.getSerializedSize()
                                              : frontCodedIndexedWriter.getSerializedSize());
    try (
        FileChannel fileChannelFrontCoded = FileChannel.open(
            fileFrontCoded.toPath(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE
        );
        FileSmoosher fileSmoosherFrontCoded = new FileSmoosher(smooshDirFrontCoded);
        FileChannel fileChannelGeneric = FileChannel.open(
            fileGeneric.toPath(),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE
        );
        FileSmoosher fileSmoosherGeneric = new FileSmoosher(smooshDirGeneric)
    ) {
      frontCodedIndexedWriter.writeTo(fileChannelFrontCoded, fileSmoosherFrontCoded);
      genericIndexedWriter.writeTo(fileChannelGeneric, fileSmoosherGeneric);
    }

    FileChannel fileChannelGeneric = FileChannel.open(fileGeneric.toPath());
    MappedByteBuffer byteBufferGeneric = fileChannelGeneric.map(FileChannel.MapMode.READ_ONLY, 0, fileGeneric.length());
    FileChannel fileChannelFrontCoded = FileChannel.open(fileFrontCoded.toPath());
    MappedByteBuffer byteBufferFrontCoded = fileChannelFrontCoded.map(
        FileChannel.MapMode.READ_ONLY,
        0,
        fileFrontCoded.length()
    );

    genericIndexed = GenericIndexed.read(
        byteBufferGeneric,
        GenericIndexed.BYTE_BUFFER_STRATEGY,
        SmooshedFileMapper.load(smooshDirFrontCoded)
    );
    frontCodedIndexed = FrontCodedIndexed.read(
        byteBufferFrontCoded.order(ByteOrder.nativeOrder()),
        ByteOrder.nativeOrder()
    ).get();

    // sanity test
    for (int i = 0; i < numElements; i++) {
      final String expected = StringUtils.fromUtf8Nullable(genericIndexed.get(i));
      final String actual = StringUtils.fromUtf8Nullable(frontCodedIndexed.get(i));
      Preconditions.checkArgument(
          Objects.equals(expected, actual),
          "elements not equal: " + i + " " + expected + " " + actual
      );
    }

    Iterator<ByteBuffer> genericIterator = genericIndexed.iterator();
    Iterator<ByteBuffer> frontCodedIterator = frontCodedIndexed.iterator();
    Iterator<String> frontCodedStringIterator =
        new StringEncodingStrategies.Utf8ToStringIndexed(frontCodedIndexed).iterator();

    int counter = 0;
    while (genericIterator.hasNext() && frontCodedIterator.hasNext() && frontCodedStringIterator.hasNext()) {
      final String expected = StringUtils.fromUtf8Nullable(genericIterator.next());
      final String actual = StringUtils.fromUtf8Nullable(frontCodedIterator.next());
      final String actual2 = frontCodedStringIterator.next();
      Preconditions.checkArgument(
          Objects.equals(expected, actual),
          "elements not equal: " + counter + " " + expected + " " + actual
      );
      Preconditions.checkArgument(
          Objects.equals(expected, actual2),
          "elements not equal: " + counter + " " + expected + " " + actual
      );
      counter++;
    }
    Preconditions.checkArgument(counter == numElements);
    Preconditions.checkArgument(genericIterator.hasNext() == frontCodedIterator.hasNext());
    Preconditions.checkArgument(genericIterator.hasNext() == frontCodedStringIterator.hasNext());

    elementsToSearch = new String[numOperations];
    for (int i = 0; i < numOperations; i++) {
      elementsToSearch[i] = values[ThreadLocalRandom.current().nextInt(numElements)];
    }
    iterationIndexes = new int[numOperations];
    for (int i = 0; i < numOperations; i++) {
      iterationIndexes[i] = ThreadLocalRandom.current().nextInt(numElements);
    }
    if ("generic".equals(indexType)) {
      indexed = genericIndexed.singleThreaded();
    } else {
      indexed = frontCodedIndexed;
    }
  }

  @Benchmark
  public void get(Blackhole bh)
  {
    for (int i : iterationIndexes) {
      bh.consume(indexed.get(i));
    }
  }

  @Benchmark
  public int indexOf()
  {
    int r = 0;
    for (String elementToSearch : elementsToSearch) {
      r ^= indexed.indexOf(StringUtils.toUtf8ByteBuffer(elementToSearch));
    }
    return r;
  }

  @Benchmark
  public void iterator(Blackhole blackhole)
  {
    final Iterator<ByteBuffer> iterator = indexed.iterator();
    while (iterator.hasNext()) {
      final ByteBuffer buffer = iterator.next();
      if (buffer == null) {
        blackhole.consume(null);
      } else {
        blackhole.consume(StringUtils.fromUtf8(buffer));
      }
    }
  }


  private static String getRandomId(int width)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < width; ++i) {
      suffix.append((char) ('a' + ((ThreadLocalRandom.current().nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(FrontCodedIndexedBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
