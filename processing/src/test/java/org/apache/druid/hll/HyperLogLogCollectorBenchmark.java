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

package org.apache.druid.hll;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.UnsafeUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * TODO rewrite to use JMH and move to the benchmarks project
 */
public class HyperLogLogCollectorBenchmark extends SimpleBenchmark
{
  private final HashFunction fn = Hashing.murmur3_128();

  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") // TODO understand if this field should be used or not
  private final List<HyperLogLogCollector> collectors = new ArrayList<>();

  @Param({"true"}) boolean targetIsDirect;
  @Param({"default", "random", "0"}) String alignment;

  boolean alignSource;
  boolean alignTarget;

  int cacheLine = 64;

  ByteBuffer chunk;
  final int count = 100_000;
  int[] positions = new int[count];
  int[] sizes = new int[count];

  @Override
  protected void setUp()
  {
    boolean random = false;
    Random rand = new Random(0);
    int defaultOffset = 0;

    switch (alignment) {
      case "default":
        alignSource = false;
        alignTarget = false;
        break;

      case "random":
        random = true;
        break;

      default:
        defaultOffset = Integer.parseInt(alignment);
    }

    int val = 0;
    chunk = ByteBuffers.allocateAlignedByteBuffer(
        (HyperLogLogCollector.getLatestNumBytesForDenseStorage() + cacheLine
         + cacheLine) * count, cacheLine
    );

    int pos = 0;
    for (int i = 0; i < count; ++i) {
      HyperLogLogCollector c = HyperLogLogCollector.makeLatestCollector();
      for (int k = 0; k < 40; ++k) {
        c.add(fn.hashInt(++val).asBytes());
      }
      final ByteBuffer sparseHeapCopy = c.toByteBuffer();
      int size = sparseHeapCopy.remaining();

      final ByteBuffer buf;

      final int offset = random ? (int) (rand.nextDouble() * 64) : defaultOffset;

      if (alignSource && (pos % cacheLine) != offset) {
        pos += (pos % cacheLine) < offset ? offset - (pos % cacheLine) : (cacheLine + offset - pos % cacheLine);
      }

      positions[i] = pos;
      sizes[i] = size;

      chunk.limit(pos + size);
      chunk.position(pos);
      buf = chunk.duplicate();
      buf.mark();

      pos += size;

      buf.put(sparseHeapCopy);
      buf.reset();
      collectors.add(HyperLogLogCollector.makeCollector(buf));
    }
  }

  private ByteBuffer allocateEmptyHLLBuffer(boolean direct, boolean aligned, int offset)
  {
    final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    final byte[] emptyBytes = HyperLogLogCollector.makeEmptyVersionedByteArray();
    final ByteBuffer buf;
    if (direct) {
      if (aligned) {
        buf = ByteBuffers.allocateAlignedByteBuffer(size + offset, cacheLine);
        buf.position(offset);
        buf.mark();
        buf.limit(size + offset);
      } else {
        buf = ByteBuffer.allocateDirect(size);
        buf.mark();
        buf.limit(size);
      }

      buf.put(emptyBytes);
      buf.reset();
    } else {
      buf = ByteBuffer.allocate(size);
      buf.limit(size);
      buf.put(emptyBytes);
      buf.rewind();
    }
    return buf;
  }

  @SuppressWarnings("unused") // Supposedly called by Caliper
  public double timeFold(int reps)
  {
    final ByteBuffer buf = allocateEmptyHLLBuffer(targetIsDirect, alignTarget, 0);

    for (int k = 0; k < reps; ++k) {
      for (int i = 0; i < count; ++i) { //-V6017: The 'k' counter is not used the nested loop because it's just reps.
        final int pos = positions[i];
        final int size = sizes[i];

        HyperLogLogCollector.makeCollector(
            (ByteBuffer) buf.duplicate().position(0).limit(
                HyperLogLogCollector.getLatestNumBytesForDenseStorage()
            )
        ).fold(
            HyperLogLogCollector.makeCollector(
                (ByteBuffer) chunk.duplicate().limit(pos + size).position(pos)
            )
        );
      }
    }
    return HyperLogLogCollector.makeCollector(buf.duplicate()).estimateCardinality();
  }

  public static void main(String[] args)
  {
    Runner.main(HyperLogLogCollectorBenchmark.class, args);
  }
}

class ByteBuffers
{
  private static final long ADDRESS_OFFSET;
  private static final MethodHandle GET_LONG;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      ADDRESS_OFFSET = lookupAddressOffset(lookup);
      GET_LONG = lookupGetLong(lookup);
    }
    catch (Throwable t) {
      throw new RuntimeException("Unable to lookup Unsafe methods", t);
    }
  }

  private static long lookupAddressOffset(MethodHandles.Lookup lookup) throws Throwable
  {
    MethodHandle objectFieldOffset = lookup.findVirtual(UnsafeUtils.theUnsafeClass(), "objectFieldOffset",
                                                        MethodType.methodType(long.class, Field.class)
    );
    return (long) objectFieldOffset.bindTo(UnsafeUtils.theUnsafe()).invoke(Buffer.class.getDeclaredField("address"));
  }

  private static MethodHandle lookupGetLong(MethodHandles.Lookup lookup) throws Throwable
  {
    MethodHandle getLong = lookup.findVirtual(UnsafeUtils.theUnsafeClass(), "getLong",
                                              MethodType.methodType(long.class, Object.class, long.class)
    );
    return getLong.bindTo(UnsafeUtils.theUnsafe());
  }

  public static long getAddress(ByteBuffer buf)
  {
    try {
      return (long) GET_LONG.invoke(buf, ADDRESS_OFFSET);
    }
    catch (Throwable t) {
      throw new UnsupportedOperationException("Unsafe.getLong is unsupported", t);
    }
  }

  public static ByteBuffer allocateAlignedByteBuffer(int capacity, int align)
  {
    Preconditions.checkArgument(Long.bitCount(align) == 1, "Alignment must be a power of 2");
    final ByteBuffer buf = ByteBuffer.allocateDirect(capacity + align);
    long address = getAddress(buf);
    if ((address & (align - 1)) == 0) {
      buf.limit(capacity);
    } else {
      int offset = (int) (align - (address & (align - 1)));
      buf.position(offset);
      buf.limit(offset + capacity);
    }
    return buf.slice();
  }
}
