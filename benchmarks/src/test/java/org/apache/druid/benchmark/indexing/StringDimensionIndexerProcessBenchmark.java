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

package org.apache.druid.benchmark.indexing;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.StringDimensionIndexer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class StringDimensionIndexerProcessBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  String[] inputData;
  StringDimensionIndexer emptyIndexer;
  StringDimensionIndexer fullIndexer;
  int[] readOrder;

  @Setup(Level.Trial)
  public void setup()
  {
    // maxNumbers : inputData ratio of 1000:50000 is 1:50, in other words we should expect each element to be 'added'
    // 50x, the first time it is new, the next 49 times it is an existing entry; also 5% of values will be false

    final int maxNumbers = 1000;
    final int nullNumbers = 50; // 5%
    final int validNumbers = (maxNumbers + 1) - nullNumbers;

    // set up dummy input data, and load to indexer
    inputData = new String[50000];
    for (int i = 0; i < inputData.length; i++) {
      int next = ThreadLocalRandom.current().nextInt(maxNumbers);
      inputData[i] = (next < nullNumbers) ? null : ("abcd-" + next + "-efgh");
    }

    fullIndexer = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true, false, false);
    for (String data : inputData) {
      fullIndexer.processRowValsToUnsortedEncodedKeyComponent(data, true);
    }

    // set up a random read order
    readOrder = new int[inputData.length];
    for (int i = 0; i < readOrder.length; i++) {
      readOrder[i] = ThreadLocalRandom.current().nextInt(validNumbers);
    }
  }
  @Setup(Level.Iteration)
  public void setupEmptyIndexer()
  {
    emptyIndexer = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true, false, false);
  }

  @Setup(Level.Iteration)
  public void shuffleReadOrder()
  {
    ArrayList<Integer> asList = new ArrayList<>(readOrder.length);
    for (int i : readOrder) {
      asList.add(i);
    }

    Collections.shuffle(asList);

    for (int i = 0; i < readOrder.length; i++) {
      readOrder[i] = asList.get(i);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void soloWriter()
  {
    // load ALL input data to an empty index; duplicates will be present / should be ignored
    for (String data : inputData) {
      emptyIndexer.processRowValsToUnsortedEncodedKeyComponent(data, true);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Group("soloReader")
  public void soloReader(Blackhole blackhole)
  {
    // read ALL elements from a fully loaded index
    for (int i : readOrder) {
      Object result = fullIndexer.convertUnsortedEncodedKeyComponentToActualList(new int[]{i});
      blackhole.consume(result);
    }
  }

  // parallel read/write test should simulate what happens when we are (1) ingesting data (aka writing to dictionary)
  // and also (2) running query (aka reading from dictionary)
  // the read side should continuously read
  // the write side should continuously write; but we also need to throw in some random writes too
  // since our dataset will fill the 'new write' path quickly, so 1-in-50 elements should be new

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Group("parallelReadWrite")
  public void parallelWriter()
  {
    int count = 0;
    // load ALL input data to an empty index; duplicates will be present / should be ignored
    for (String data : inputData) {
      fullIndexer.processRowValsToUnsortedEncodedKeyComponent(data, true);
      count++;
      if (count == 50) {
        int next = ThreadLocalRandom.current().nextInt(10000);
        fullIndexer.processRowValsToUnsortedEncodedKeyComponent("xxx-" + next + "yz", true);
        count = 0;
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Group("parallelReadWrite")
  @GroupThreads(3)
  public void parallelReader(Blackhole blackhole)
  {
    for (int i : readOrder) {
      Object result = fullIndexer.convertUnsortedEncodedKeyComponentToActualList(new int[]{i});
      blackhole.consume(result);
    }
  }
}
