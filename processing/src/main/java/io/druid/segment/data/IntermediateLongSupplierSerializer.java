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

package io.druid.segment.data;

import com.google.common.math.LongMath;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Unsafe for concurrent use from multiple threads.
 */
public class IntermediateLongSupplierSerializer implements LongSupplierSerializer
{
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ByteOrder order;
  private final CompressionStrategy compression;
  private LongList tempOut = null;

  private int numInserted = 0;

  private final Long2IntMap uniqueValues = new Long2IntOpenHashMap();
  private final LongList valuesAddedInOrder = new LongArrayList();

  private long maxVal = Long.MIN_VALUE;
  private long minVal = Long.MAX_VALUE;

  private LongSupplierSerializer delegate;

  IntermediateLongSupplierSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder order,
      CompressionStrategy compression
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.order = order;
    this.compression = compression;
  }

  @Override
  public void open() throws IOException
  {
    tempOut = new LongArrayList();
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(long value) throws IOException
  {
    //noinspection VariableNotUsedInsideIf
    if (delegate != null) {
      throw new IllegalStateException("written out already");
    }
    tempOut.add(value);
    ++numInserted;
    if (uniqueValues.size() <= CompressionFactory.MAX_TABLE_SIZE && !uniqueValues.containsKey(value)) {
      uniqueValues.put(value, uniqueValues.size());
      valuesAddedInOrder.add(value);
    }
    if (value > maxVal) {
      maxVal = value;
    }
    if (value < minVal) {
      minVal = value;
    }
  }

  private void makeDelegate() throws IOException
  {
    //noinspection VariableNotUsedInsideIf
    if (delegate != null) {
      return;
    }
    CompressionFactory.LongEncodingWriter writer;
    long delta;
    try {
      delta = LongMath.checkedSubtract(maxVal, minVal);
    }
    catch (ArithmeticException e) {
      delta = -1;
    }
    if (uniqueValues.size() <= CompressionFactory.MAX_TABLE_SIZE) {
      writer = new TableLongEncodingWriter(uniqueValues, valuesAddedInOrder);
    } else if (delta != -1 && delta != Long.MAX_VALUE) {
      writer = new DeltaLongEncodingWriter(minVal, delta);
    } else {
      writer = new LongsLongEncodingWriter(order);
    }

    if (compression == CompressionStrategy.NONE) {
      delegate = new EntireLayoutLongSupplierSerializer(segmentWriteOutMedium, writer);
    } else {
      delegate = new BlockLayoutLongSupplierSerializer(segmentWriteOutMedium, filenameBase, order, writer, compression);
    }

    delegate.open();
    for (int i = 0; i < tempOut.size(); i++) {
      delegate.add(tempOut.getLong(i));
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    makeDelegate();
    return delegate.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    makeDelegate();
    delegate.writeTo(channel, smoosher);
  }
}
