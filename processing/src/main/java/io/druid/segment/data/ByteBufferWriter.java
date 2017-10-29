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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 */
public class ByteBufferWriter<T> implements Closeable
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valueOut = null;
  private final ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES);

  public ByteBufferWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
  }

  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valueOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("value")));
  }

  public void write(T objectToWrite) throws IOException
  {
    byte[] bytesToWrite = strategy.toBytes(objectToWrite);
    SerializerUtils.writeBigEndianIntToOutputStream(headerOut, bytesToWrite.length, helperBuffer);
    valueOut.write(bytesToWrite);
  }

  private String makeFilename(String suffix)
  {
    return StringUtils.format("%s.%s", filenameBase, suffix);
  }

  @Override
  public void close() throws IOException
  {
    headerOut.close();
    valueOut.close();

    final long numBytesWritten = headerOut.getCount() + valueOut.getCount();
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );
  }

  public long getSerializedSize()
  {
    return headerOut.getCount() + valueOut.getCount();
  }

  public InputSupplier<InputStream> combineStreams()
  {
    return ByteStreams.join(
        Iterables.transform(
            Arrays.asList("header", "value"),
            new Function<String, InputSupplier<InputStream>>()
            {
              @Override
              public InputSupplier<InputStream> apply(final String input)
              {
                return new InputSupplier<InputStream>()
                {
                  @Override
                  public InputStream getInput() throws IOException
                  {
                    return ioPeon.makeInputStream(makeFilename(input));
                  }
                };
              }
            }
        )
    );
  }

  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    try (final ReadableByteChannel from = Channels.newChannel(combineStreams().getInput())) {
      ByteStreams.copy(from, channel);
    }
  }
}
