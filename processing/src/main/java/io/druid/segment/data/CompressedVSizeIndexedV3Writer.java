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

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.CompressedVSizeIndexedV3Supplier;
import io.druid.segment.IndexIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class CompressedVSizeIndexedV3Writer extends MultiValueIndexedIntsWriter
{
  private static final byte VERSION = CompressedVSizeIndexedV3Supplier.VERSION;

  private static final List<Integer> EMPTY_LIST = new ArrayList<>();

  public static CompressedVSizeIndexedV3Writer create(
      final IOPeon ioPeon,
      final String filenameBase,
      final int maxValue,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    return new CompressedVSizeIndexedV3Writer(
        new CompressedIntsIndexedWriter(
            ioPeon,
            StringUtils.format("%s.offsets", filenameBase),
            CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER,
            IndexIO.BYTE_ORDER,
            compression
        ),
        new CompressedVSizeIntsIndexedWriter(
            ioPeon,
            StringUtils.format("%s.values", filenameBase),
            maxValue,
            CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue),
            IndexIO.BYTE_ORDER,
            compression
        )
    );
  }

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

  @Override
  public void open() throws IOException
  {
    offsetWriter.open();
    valueWriter.open();
  }

  @Override
  protected void addValues(List<Integer> vals) throws IOException
  {
    if (vals == null) {
      vals = EMPTY_LIST;
    }
    offsetWriter.add(offset);
    for (Integer val : vals) {
      valueWriter.add(val);
    }
    offset += vals.size();
  }

  @Override
  public void close() throws IOException
  {
    try {
      offsetWriter.add(offset);
    }
    finally {
      offsetWriter.close();
      valueWriter.close();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +   // version
           offsetWriter.getSerializedSize() +
           valueWriter.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    offsetWriter.writeToChannel(channel, smoosher);
    valueWriter.writeToChannel(channel, smoosher);
  }
}
