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

package org.apache.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
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
  private ByteBuffer buffer;

  public TestColumnCompression(CompressionStrategy strategy)
  {
    compressionType = strategy;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> compressionStrategies()
  {
    return Arrays.stream(CompressionStrategy.values())
                 .filter(x -> !CompressionStrategy.NONE.equals(x))
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

    buffer = serialize(
        CompressedVSizeColumnarMultiIntsSupplier.fromIterable(
            Iterables.transform(rows, (Function<int[], ColumnarInts>) input -> VSizeColumnarInts.fromArray(input, 20)),
            bound - 1,
            ByteOrder.nativeOrder(),
            compressionType,
            Closer.create()
        )
    );
    this.compressed = CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
        buffer,
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

  @After
  public void tearDown() throws IOException
  {
    ByteBufferUtils.free(buffer);
    compressed.close();
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
      public void close()
      {

      }
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
