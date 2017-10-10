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
import com.google.common.primitives.Ints;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class EntireLayoutLongSupplierSerializer implements LongSupplierSerializer
{

  private final IOPeon ioPeon;
  private final String valueFile;
  private final String metaFile;
  private CountingOutputStream valuesOut;
  private final CompressionFactory.LongEncodingWriter writer;
  private long metaCount = 0;

  private int numInserted = 0;

  public EntireLayoutLongSupplierSerializer(
      IOPeon ioPeon,
      String filenameBase,
      CompressionFactory.LongEncodingWriter writer
  )
  {
    this.ioPeon = ioPeon;
    this.valueFile = filenameBase + ".value";
    this.metaFile = filenameBase + ".format";
    this.writer = writer;
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
    writer.setOutputStream(valuesOut);
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(long value) throws IOException
  {
    writer.write(value);
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
  public void close() throws IOException
  {
    writer.flush();
    valuesOut.close();
    try (CountingOutputStream metaOut = new CountingOutputStream(ioPeon.makeOutputStream(metaFile))) {
      metaOut.write(CompressedLongsIndexedSupplier.version);
      metaOut.write(Ints.toByteArray(numInserted));
      metaOut.write(Ints.toByteArray(0));
      writer.putMeta(metaOut, CompressedObjectStrategy.CompressionStrategy.NONE);
      metaOut.close();
      metaCount = metaOut.getCount();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return metaCount + valuesOut.getCount();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    try (InputStream meta = ioPeon.makeInputStream(metaFile);
         InputStream value = ioPeon.makeInputStream(valueFile)) {
      ByteStreams.copy(Channels.newChannel(meta), channel);
      ByteStreams.copy(Channels.newChannel(value), channel);
    }
  }
}
