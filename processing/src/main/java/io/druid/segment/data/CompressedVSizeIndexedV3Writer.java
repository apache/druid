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

/**
 * Streams array of integers out in the binary format described by CompressedVSizeIndexedV3Supplier
 */
package io.druid.segment.data;

import io.druid.segment.CompressedVSizeIndexedV3Supplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class CompressedVSizeIndexedV3Writer
{
  public static final byte version = CompressedVSizeIndexedV3Supplier.version;

  private final CompressedIntsIndexedWriter offsetWriter;
  private final CompressedVSizeIntsIndexedWriter valueWriter;
  private int offset;

  public CompressedVSizeIndexedV3Writer(
      CompressedIntsIndexedWriter offsetWriter,
      CompressedVSizeIntsIndexedWriter valueWriter
  )
  {
    this.offsetWriter = offsetWriter;
    this.valueWriter = valueWriter;
    this.offset = 0;
  }

  public void open() throws IOException
  {
    offsetWriter.open();
    valueWriter.open();
  }

  public void add(int[] vals) throws IOException
  {
    offsetWriter.add(offset);
    for (int val : vals) {
      valueWriter.add(val);
    }
    offset += vals.length;
  }

  public long closeAndWriteToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    offsetWriter.add(offset);
    long offsetLen = offsetWriter.closeAndWriteToChannel(channel);
    long dataLen = valueWriter.closeAndWriteToChannel(channel);
    return 1 + offsetLen + dataLen;
  }
}
