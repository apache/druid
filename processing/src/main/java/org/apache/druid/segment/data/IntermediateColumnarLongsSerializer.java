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

import com.google.common.math.LongMath;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Serializer that chooses optimal {@link ColumnarLongs} format dymamically, based on the values being written.
 *
 * Unsafe for concurrent use from multiple threads.
 */
public class IntermediateColumnarLongsSerializer implements ColumnarLongsSerializer
{
  private final String columnName;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ByteOrder order;
  private final CompressionStrategy compression;

  private int numInserted = 0;

  private final Long2IntMap uniqueValues = new Long2IntOpenHashMap();
  private final LongList valuesAddedInOrder = new LongArrayList();

  private long maxVal = Long.MIN_VALUE;
  private long minVal = Long.MAX_VALUE;

  @Nullable
  private LongList tempOut = null;
  @Nullable
  private ColumnarLongsSerializer delegate;

  IntermediateColumnarLongsSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder order,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.order = order;
    this.compression = compression;
  }

  @Override
  public void open()
  {
    tempOut = new LongArrayList();
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(long value)
  {
    //noinspection VariableNotUsedInsideIf
    if (delegate != null) {
      throw new IllegalStateException("written out already");
    }
    tempOut.add(value);
    ++numInserted;
    if (numInserted < 0) {
      throw new ColumnCapacityExceededException(columnName);
    }
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
      delegate = new EntireLayoutColumnarLongsSerializer(columnName, segmentWriteOutMedium, writer);
    } else {
      delegate = new BlockLayoutColumnarLongsSerializer(
          columnName,
          segmentWriteOutMedium,
          filenameBase,
          order,
          writer,
          compression
      );
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
