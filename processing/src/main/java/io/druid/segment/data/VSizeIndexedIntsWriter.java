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

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Streams integers out in the binary format described by VSizeIndexedInts
 */
public class VSizeIndexedIntsWriter extends SingleValueIndexedIntsWriter
{
  private static final byte VERSION = VSizeIndexedInts.VERSION;

  private final IOPeon ioPeon;
  private final String valueFileName;
  private final int numBytes;

  private CountingOutputStream valuesOut = null;
  private final ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES);

  public VSizeIndexedIntsWriter(
      final IOPeon ioPeon,
      final String filenameBase,
      final int maxValue
  )
  {
    this.ioPeon = ioPeon;
    this.valueFileName = StringUtils.format("%s.values", filenameBase);
    this.numBytes = VSizeIndexedInts.getNumBytesForMax(maxValue);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFileName));
  }

  @Override
  protected void addValue(int val) throws IOException
  {
    helperBuffer.putInt(0, val);
    valuesOut.write(helperBuffer.array(), Ints.BYTES - numBytes, numBytes);
  }

  @Override
  public void close() throws IOException
  {
    byte[] bufPadding = new byte[4 - numBytes];
    valuesOut.write(bufPadding);
    valuesOut.close();
  }

  @Override
  public long getSerializedSize()
  {
    return 2 +       // version and numBytes
           4 +       // dataLen
           valuesOut.getCount();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    long numBytesWritten = valuesOut.getCount();
    channel.write(ByteBuffer.wrap(new byte[]{VERSION, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray((int) numBytesWritten)));
    try (final ReadableByteChannel from = Channels.newChannel(ioPeon.makeInputStream(valueFileName))) {
      ByteStreams.copy(from, channel);
    }
  }
}
