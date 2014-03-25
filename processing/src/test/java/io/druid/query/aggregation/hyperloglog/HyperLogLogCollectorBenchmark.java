package io.druid.query.aggregation.hyperloglog;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class HyperLogLogCollectorBenchmark extends SimpleBenchmark
{
  private final HashFunction fn = Hashing.murmur3_128();

  private final List<HyperLogLogCollector> collectors = Lists.newLinkedList();

  @Param({"true"}) boolean targetIsDirect;
  @Param({"0"}) int offset;

  boolean alignSource;
  boolean alignTarget;

  int CACHE_LINE = 64;

  ByteBuffer chunk;
  final int count = 100_000;
  int[] positions = new int[count];
  int[] sizes = new int[count];

  @Override
  protected void setUp() throws Exception
  {
    boolean align = true;
    if(offset < 0) {
      align = false;
      offset = 0;
    }
    alignSource = align;
    alignTarget = align;

    int val = 0;
    chunk = ByteBuffers.allocateAlignedByteBuffer(
        (HyperLogLogCollector.getLatestNumBytesForDenseStorage() + CACHE_LINE
         + offset) * count, CACHE_LINE
    );

    int pos = 0;
    for(int i = 0; i < count; ++i) {
      HyperLogLogCollector c = HyperLogLogCollector.makeLatestCollector();
      for(int k = 0; k < 40; ++k) c.add(fn.hashInt(++val).asBytes());
      final ByteBuffer sparseHeapCopy = c.toByteBuffer();
      int size = sparseHeapCopy.remaining();

      final ByteBuffer buf;

      if(alignSource && (pos % CACHE_LINE) != offset) {
        pos += (pos % CACHE_LINE) < offset ? offset - (pos % CACHE_LINE) : (CACHE_LINE + offset - pos % CACHE_LINE);
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

  private HyperLogLogCollector allocateCollector(boolean direct, boolean aligned)
  {
    final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
    final ByteBuffer buf;
    if(direct) {
      if(aligned) {
        buf = ByteBuffers.allocateAlignedByteBuffer(size + offset, CACHE_LINE);
        buf.position(offset);
        buf.mark();
        buf.limit(size + offset);
      } else {
        buf = ByteBuffer.allocateDirect(size);
        buf.mark();
        buf.limit(size);
      }

      buf.put(EMPTY_BYTES);
      buf.reset();
    }
    else {
      buf = ByteBuffer.allocate(size);
      buf.limit(size);
      buf.put(EMPTY_BYTES);
      buf.rewind();
    }
    return HyperLogLogCollector.makeCollector(buf);
  }

  public double timeFoldDirect(int reps) throws Exception
  {
    final HyperLogLogCollector rolling = allocateCollector(targetIsDirect, alignTarget);

    for (int k = 0; k < reps; ++k) {
      for(int i = 0; i < count; ++i) {
        final int pos = positions[i];
        final int size = sizes[i];
        rolling.fold(HyperLogLogCollector.makeCollector(
                         (ByteBuffer) chunk.limit(pos+size).position(pos)
        ));
      }
    }
    return rolling.estimateCardinality();
  }

//  public double timeFoldIterable(int reps) {
//    HyperLogLogCollector rolling = allocateCollector(targetIsDirect, align);
//
//    final Iterator<HyperLogLogCollector> it = Iterables.cycle(collectors).iterator();
//    for(int k = 0; k < reps; ++k) {
//      for(int i = 0; i < count; ++i) {
//        rolling.fold(it.next());
//      }
//    }
//    return rolling.estimateCardinality();
//  }

  public static void main(String[] args) throws Exception {
//    HyperLogLogCollectorBenchmark benchmark = new HyperLogLogCollectorBenchmark();
//    benchmark.foldCount = 100;
//    benchmark.setUp();
//    double d = benchmark.timeFold(10_000);
//    System.out.println(d);
    Runner.main(HyperLogLogCollectorBenchmark.class, args);
  }
}

class ByteBuffers {
  private static final Unsafe UNSAFE;
  private static final long ADDRESS_OFFSET;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
    } catch (Exception e) {
      throw new RuntimeException("Cannot access Unsafe methods", e);
    }
  }

  public static long getAddress(ByteBuffer buf) {
    return UNSAFE.getLong(buf, ADDRESS_OFFSET);
  }

  public static ByteBuffer allocateAlignedByteBuffer(int capacity, int align) {
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
