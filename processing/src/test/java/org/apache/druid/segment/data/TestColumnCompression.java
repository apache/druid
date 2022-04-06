package org.apache.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.logging.log4j.core.layout.GelfLayout;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TestColumnCompression
{

  private final CompressionStrategy compressionType;
  private ColumnarMultiInts compressed;
  int bytes = 1;
  int valuesPerRowBound = 5;
  int filteredRowCount = 1000;
  private BitSet filter;

  public TestColumnCompression(CompressionStrategy strategy)
  {
    compressionType = strategy;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> compressionStrategies()
  {
    return Arrays.stream(CompressionStrategy.values())
                 .map(strategy -> new Object[]{strategy}).collect(Collectors.toList());
  }

  @Before
  public void setUp() throws Exception
  {
    Random rand = ThreadLocalRandom.current();
    List<int[]> rows = new ArrayList<>();
    final int bound = 1 << bytes;
    for (int i = 0; i < 0x100000; i++) {
      int count = rand.nextInt(valuesPerRowBound) + 1;
      int[] row = new int[rand.nextInt(count)];
      for (int j = 0; j < row.length; j++) {
        row[j] = rand.nextInt(bound);
      }
      rows.add(row);
    }

    final ByteBuffer bufferCompressed = serialize(
        CompressedVSizeColumnarMultiIntsSupplier.fromIterable(
            Iterables.transform(rows, (Function<int[], ColumnarInts>) input -> VSizeColumnarInts.fromArray(input, 20)),
            bound - 1,
            ByteOrder.nativeOrder(),
            compressionType,
            Closer.create()
        )
    );
    this.compressed = CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
        bufferCompressed,
        ByteOrder.nativeOrder()
    ).get();

    filter = new BitSet();
    for (int i = 0; i < filteredRowCount; i++) {
      int rowToAccess = rand.nextInt(rows.size());
      // Skip already selected rows if any
      while (filter.get(rowToAccess)) {
        rowToAccess = (rowToAccess + 1) % rows.size();
      }
      filter.set(rowToAccess);
    }
  }

  private static ByteBuffer serialize(WritableSupplier<ColumnarMultiInts> writableSupplier) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocateDirect((int) writableSupplier.getSerializedSize());
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
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
      public void close() { }
    };

    writableSupplier.writeTo(channel, null);
    buffer.rewind();
    return buffer;
  }

  @Test
  public void testCompressed()
  {
    for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
      IndexedInts row = compressed.get(i);
      for (int j = 0, rowSize = row != null ? row.size() : 0; j < rowSize; j++) {
        row.get(j);
      }
    }
  }
}
