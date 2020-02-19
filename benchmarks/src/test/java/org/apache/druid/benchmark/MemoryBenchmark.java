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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.epinephelinae.collection.HashTableUtils;
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
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 15)
public class MemoryBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"4", "5", "8", "9", "12", "16", "31", "32", "64", "128"})
  public int numBytes;

  @Param({"offheap"})
  public String where;

  private ByteBuffer buffer1;
  private ByteBuffer buffer2;
  private ByteBuffer buffer3;
  private WritableMemory memory1;
  private WritableMemory memory2;
  private WritableMemory memory3;

  @Setup
  public void setUp()
  {
    if ("onheap".equals(where)) {
      buffer1 = ByteBuffer.allocate(numBytes).order(ByteOrder.nativeOrder());
      buffer2 = ByteBuffer.allocate(numBytes).order(ByteOrder.nativeOrder());
      buffer3 = ByteBuffer.allocate(numBytes).order(ByteOrder.nativeOrder());
    } else if ("offheap".equals(where)) {
      buffer1 = ByteBuffer.allocateDirect(numBytes).order(ByteOrder.nativeOrder());
      buffer2 = ByteBuffer.allocateDirect(numBytes).order(ByteOrder.nativeOrder());
      buffer3 = ByteBuffer.allocateDirect(numBytes).order(ByteOrder.nativeOrder());
    }

    memory1 = WritableMemory.wrap(buffer1, ByteOrder.nativeOrder());
    memory2 = WritableMemory.wrap(buffer2, ByteOrder.nativeOrder());
    memory3 = WritableMemory.wrap(buffer3, ByteOrder.nativeOrder());

    // Scribble in some random but consistent (same seed) garbage.
    final Random random = new Random(0);
    for (int i = 0; i < numBytes; i++) {
      memory1.putByte(i, (byte) random.nextInt());
    }

    // memory1 == memory2
    memory1.copyTo(0, memory2, 0, numBytes);

    // memory1 != memory3, but only slightly (different in a middle byte; an attempt to not favor leftward moving vs
    // rightward moving equality checks).
    memory1.copyTo(0, memory3, 0, numBytes);
    memory3.putByte(numBytes / 2, (byte) (~memory3.getByte(numBytes / 2)));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_byteBuffer_whenEqual(Blackhole blackhole)
  {
    blackhole.consume(buffer1.equals(buffer2));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_byteBuffer_whenDifferent(Blackhole blackhole)
  {
    blackhole.consume(buffer1.equals(buffer3));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_hashTableUtils_whenEqual(Blackhole blackhole)
  {
    blackhole.consume(HashTableUtils.memoryEquals(memory1, 0, memory2, 0, numBytes));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_hashTableUtils_whenDifferent(Blackhole blackhole)
  {
    blackhole.consume(HashTableUtils.memoryEquals(memory1, 0, memory3, 0, numBytes));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_memoryEqualTo_whenEqual(Blackhole blackhole)
  {
    blackhole.consume(memory1.equalTo(0, memory2, 0, numBytes));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals_memoryEqualTo_whenDifferent(Blackhole blackhole)
  {
    blackhole.consume(memory1.equalTo(0, memory3, 0, numBytes));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hash_byteBufferHashCode(Blackhole blackhole)
  {
    blackhole.consume(buffer1.hashCode());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hash_hashTableUtils(Blackhole blackhole)
  {
    blackhole.consume(HashTableUtils.hashMemory(memory1, 0, numBytes));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void hash_memoryXxHash64(Blackhole blackhole)
  {
    blackhole.consume(memory1.xxHash64(0, numBytes, 0));
  }
}
