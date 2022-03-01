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

import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.SparseArrayIndexed;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class DictionaryWrapperBenchmark
{
  @Param({"1000000"})
  public int cardinality = 1 << 20;

  @Param({"10000"})
  public int searchNum = 10000;

  @Param({"16"})
  public int strLen = 16;

  @Param({"0", "512", "1024", "2048", "4096", "8192"})
  public int indexGranularity = 1024;

  @Param({"array"})
  public String sparseType = "array";

  private File file;
  private File smooshDir;
  private Indexed<String> dictionary;

  private String[] strings;
  private List<String> elementsToSearch;

  @Setup
  public void setup() throws IOException
  {
    NullHandling.initializeForTests();

    strings = Iterables.toArray(getStringSet(cardinality, strLen, 0, 128), String.class);
    Arrays.sort(strings, GenericIndexed.STRING_STRATEGY);
    double prob = 1.0D * searchNum / cardinality;
    elementsToSearch = new ArrayList<>(searchNum);
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (String str : strings) {
      if (random.nextDouble() < prob) {
        elementsToSearch.add(NullHandling.emptyToNullIfNeeded(str));
      }
    }
    setupDictionary();
  }

  private void setupDictionary() throws IOException
  {
    String filename = UUID.randomUUID().toString();
    GenericIndexedWriter<String> genericIndexedWriter = new GenericIndexedWriter<>(
        new OffHeapMemorySegmentWriteOutMedium(),
        filename,
        GenericIndexed.STRING_STRATEGY
    );
    genericIndexedWriter.open();

    for (String str : strings) {
      genericIndexedWriter.write(str);
    }
    smooshDir = FileUtils.createTempDir();
    file = File.createTempFile(filename, "meta");

    try (FileChannel fileChannel =
             FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
         FileSmoosher fileSmoosher = new FileSmoosher(smooshDir)) {
      genericIndexedWriter.writeTo(fileChannel, fileSmoosher);
    }

    FileChannel fileChannel = FileChannel.open(file.toPath());
    MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
    GenericIndexed<String> genericIndexes = GenericIndexed.read(
        byteBuffer,
        GenericIndexed.STRING_STRATEGY,
        SmooshedFileMapper.load(smooshDir)
    );

    if (indexGranularity > 0) {
      this.dictionary = new SparseArrayIndexed<>(genericIndexes, indexGranularity);
    } else {
      dictionary = genericIndexes;
    }
  }

  @Benchmark
  public int indexOf()
  {
    int r = 0;
    for (String elementToSearch : elementsToSearch) {
      r ^= dictionary.indexOf(elementToSearch);
    }
    return r;
  }

  private static Set<String> getStringSet(int n, int size, int min, int max)
  {
    Set<String> strings = new HashSet<>();
    ThreadLocalRandom random = ThreadLocalRandom.current();

    while (strings.size() < n) {
      strings.add(getString(random.nextInt(size), min, max));
    }
    return strings;
  }

  @Nullable
  private static String getString(int len, int min, int max)
  {
    Random random = ThreadLocalRandom.current();
    if (len == 0 && random.nextBoolean()) {
      return null;
    }
    StringBuilder builder = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      builder.append(Character.toChars(random.nextInt(max - min) + min));
    }
    return builder.toString();
  }

  /**
   * This main() is for debugging from the IDE.
   */
  public static void main(String[] args) throws RunnerException, IOException
  {
    NullHandling.initializeForTests();

    DictionaryWrapperBenchmark benchmark = new DictionaryWrapperBenchmark();
    benchmark.setup();

    for (String str : benchmark.elementsToSearch) {
      int expect = benchmark.dictionary.indexOf(str);
      int actual = Arrays.binarySearch(benchmark.strings, str, GenericIndexed.STRING_STRATEGY);
      if (expect < 0 || expect != actual) {
        throw new ISE("index of %s fail, expect:%d, actual:%d", str, expect, actual);
      }
    }

    Options opt = new OptionsBuilder()
        .include(DictionaryWrapperBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
