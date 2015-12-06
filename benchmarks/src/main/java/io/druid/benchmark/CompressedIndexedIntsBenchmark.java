/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark;

import com.google.common.primitives.Ints;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.data.WritableSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class CompressedIndexedIntsBenchmark
{
  private IndexedInts uncompressed;
  private IndexedInts compressed;

  @Param({"1", "2", "3", "4"})
  int bytes;

  // Number of rows to read, the test will read random rows
  @Param({"1000", "10000", "100000", "1000000", "1000000"})
  int filteredRowCount;

  private BitSet filter;

  @Setup
  public void setup() throws IOException
  {
    Random rand = new Random(0);
    int[] vals = new int[0x100000];
    final int bound = 1 << bytes;
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(bound);
    }
    final ByteBuffer bufferCompressed = serialize(
        CompressedVSizeIntsIndexedSupplier.fromList(
            Ints.asList(vals),
            bound - 1,
            CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForBytes(bytes),
            ByteOrder.nativeOrder(), CompressedObjectStrategy.CompressionStrategy.LZ4
        )
    );
    this.compressed = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(
        bufferCompressed, ByteOrder.nativeOrder()
    ).get();

    final ByteBuffer bufferUncompressed = serialize(
        new VSizeIndexedInts.VSizeIndexedIntsSupplier(
            VSizeIndexedInts.fromArray(
                vals
            )
        )
    );
    this.uncompressed = VSizeIndexedInts.readFromByteBuffer(bufferUncompressed);

    filter = new BitSet();
    for (int i = 0; i < filteredRowCount; i++) {
      int rowToAccess = rand.nextInt(vals.length);
      // Skip already selected rows if any
      while (filter.get(rowToAccess)) {
        rowToAccess = (rowToAccess+1) % vals.length;
      }
      filter.set(rowToAccess);
    }

  }

  private static ByteBuffer serialize(WritableSupplier<IndexedInts> writableSupplier) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocateDirect((int) writableSupplier.getSerializedSize());

    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src) throws IOException
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
      }
    };

    writableSupplier.writeToChannel(channel);
    buffer.rewind();
    return buffer;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void uncompressed(Blackhole blackhole)
  {
    for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
      blackhole.consume(uncompressed.get(i));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compressed(Blackhole blackhole)
  {
    for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
      blackhole.consume(compressed.get(i));
    }
  }
}
