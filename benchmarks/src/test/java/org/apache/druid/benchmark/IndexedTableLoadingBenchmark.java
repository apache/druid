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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.join.table.IndexedTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class IndexedTableLoadingBenchmark
{
  private static List<Set<String>> KEY_COLUMN_SETS = ImmutableList.of(
      ImmutableSet.of("stringKey", "longKey")
  );

  @Param({"0"})
  int keyColumns;

  @Param({"50000", "500000", "5000000"})
  int rowsPerSegment;

  @Param({"segment"})
  String indexedTableType;

  Closer closer = Closer.create();

  QueryableIndexSegment tableSegment = null;
  IndexedTable table = null;

  @Setup(Level.Trial)
  public void setup()
  {
    tableSegment = IndexedTableJoinCursorBenchmark.makeQueryableIndexSegment(closer, "join", rowsPerSegment);
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() throws IOException
  {
    table.close();
  }

  @TearDown
  public void tearDown() throws IOException
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void loadTable(Blackhole blackhole)
  {
    table =
        IndexedTableJoinCursorBenchmark.makeTable(indexedTableType, KEY_COLUMN_SETS.get(keyColumns), tableSegment);
    blackhole.consume(table);
  }
}
