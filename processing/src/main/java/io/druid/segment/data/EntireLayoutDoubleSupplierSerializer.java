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

import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;


public class EntireLayoutDoubleSupplierSerializer implements DoubleSupplierSerializer
{
  private final IOPeon ioPeon;
  private final String valueFile;
  private final String metaFile;
  private CountingOutputStream valuesOut;
  private long metaCount = 0;

  private final ByteBuffer orderBuffer;

  private int numInserted = 0;

  public EntireLayoutDoubleSupplierSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order)
  {
    this.ioPeon = ioPeon;
    this.valueFile = filenameBase + ".value";
    this.metaFile = filenameBase + ".format";
    this.orderBuffer = ByteBuffer.allocate(Doubles.BYTES);
    orderBuffer.order(order);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(double value) throws IOException
  {
    orderBuffer.rewind();
    orderBuffer.putDouble(value);
    valuesOut.write(orderBuffer.array());
    ++numInserted;

  }

  @Override
  public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
  {
    close();
    try (OutputStream out = consolidatedOut.openStream();
         InputStream meta = ioPeon.makeInputStream(metaFile);
         InputStream value = ioPeon.makeInputStream(valueFile)) {
      ByteStreams.copy(meta, out);
      ByteStreams.copy(value, out);
    }
  }

  @Override
  public long getSerializedSize()
  {
    return metaCount + valuesOut.getCount();
  }

  @Override
  public void writeToChannel(
      WritableByteChannel channel, FileSmoosher smoosher
  ) throws IOException
  {
    try (InputStream meta = ioPeon.makeInputStream(metaFile);
         InputStream value = ioPeon.makeInputStream(valueFile)) {
      ByteStreams.copy(Channels.newChannel(meta), channel);
      ByteStreams.copy(Channels.newChannel(value), channel);
    }
  }

  @Override
  public void close() throws IOException
  {
    valuesOut.close();
    try (CountingOutputStream metaOut = new CountingOutputStream(ioPeon.makeOutputStream(metaFile))) {
      metaOut.write(CompressedDoublesIndexedSupplier.version);
      metaOut.write(Ints.toByteArray(numInserted));
      metaOut.write(Ints.toByteArray(0));
      metaOut.write(CompressedObjectStrategy.CompressionStrategy.NONE.getId());
      metaOut.close();
      metaCount = metaOut.getCount();
    }
  }
}
