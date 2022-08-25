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

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedLongsSerializer implements Serializer
{
  private final CompressedBlockSerializer blockSerializer;
  private final ByteBuffer longValueConverter = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());

  public CompressedLongsSerializer(SegmentWriteOutMedium segmentWriteOutMedium, CompressionStrategy compression)
  {
    this.blockSerializer = new CompressedBlockSerializer(
        segmentWriteOutMedium,
        compression,
        CompressedPools.BUFFER_SIZE
    );
  }

  public void open() throws IOException
  {
    blockSerializer.open();
  }

  public void add(long value) throws IOException
  {
    longValueConverter.clear();
    longValueConverter.putLong(value);
    longValueConverter.rewind();
    blockSerializer.addValue(longValueConverter);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return blockSerializer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    blockSerializer.writeTo(channel, smoosher);
  }
}
