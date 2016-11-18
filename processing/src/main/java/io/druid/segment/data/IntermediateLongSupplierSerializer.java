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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteSink;
import com.google.common.io.CountingOutputStream;
import com.google.common.math.LongMath;
import com.google.common.primitives.Longs;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class IntermediateLongSupplierSerializer implements LongSupplierSerializer
{

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final String tempFile;
  private final ByteOrder order;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private CountingOutputStream tempOut = null;

  private int numInserted = 0;

  private BiMap<Long, Integer> uniqueValues = HashBiMap.create();
  private long maxVal = Long.MIN_VALUE;
  private long minVal = Long.MAX_VALUE;

  private LongSupplierSerializer delegate;

  public IntermediateLongSupplierSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ByteOrder order,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.ioPeon = ioPeon;
    this.tempFile = filenameBase + ".temp";
    this.filenameBase = filenameBase;
    this.order = order;
    this.compression = compression;
  }

  public void open() throws IOException
  {
    tempOut = new CountingOutputStream(ioPeon.makeOutputStream(tempFile));
  }

  public int size()
  {
    return numInserted;
  }

  public void add(long value) throws IOException
  {
    tempOut.write(Longs.toByteArray(value));
    ++numInserted;
    if (uniqueValues.size() <= CompressionFactory.MAX_TABLE_SIZE && !uniqueValues.containsKey(value)) {
      uniqueValues.put(value, uniqueValues.size());
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
    CompressionFactory.LongEncodingWriter writer;
    long delta;
    try {
      delta = LongMath.checkedSubtract(maxVal, minVal);
    }
    catch (ArithmeticException e) {
      delta = -1;
    }
    if (uniqueValues.size() <= CompressionFactory.MAX_TABLE_SIZE) {
      writer = new TableLongEncodingWriter(uniqueValues);
    } else if (delta != -1 && delta != Long.MAX_VALUE) {
      writer = new DeltaLongEncodingWriter(minVal, delta);
    } else {
      writer = new LongsLongEncodingWriter(order);
    }

    if (compression == CompressedObjectStrategy.CompressionStrategy.NONE) {
      delegate = new EntireLayoutLongSupplierSerializer(
          ioPeon, filenameBase, order, writer
      );
    } else {
      delegate = new BlockLayoutLongSupplierSerializer(
          ioPeon, filenameBase, order, writer, compression
      );
    }

    try (DataInputStream tempIn = new DataInputStream(new BufferedInputStream(ioPeon.makeInputStream(tempFile)))) {
      delegate.open();
      while (tempIn.available() > 0) {
        delegate.add(tempIn.readLong());
      }
    }
  }

  public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
  {
    tempOut.close();
    makeDelegate();
    delegate.closeAndConsolidate(consolidatedOut);
  }

  public void close() throws IOException
  {
    tempOut.close();
    makeDelegate();
    delegate.close();
  }

  public long getSerializedSize()
  {
    return delegate.getSerializedSize();
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    delegate.writeToChannel(channel);
  }
}
