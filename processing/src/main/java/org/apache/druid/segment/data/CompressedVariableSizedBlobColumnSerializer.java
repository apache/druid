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
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class CompressedVariableSizedBlobColumnSerializer implements Serializer
{
  private static final MetaSerdeHelper<CompressedVariableSizedBlobColumnSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((CompressedVariableSizedBlobColumnSerializer x) -> (byte) 0x01)
      .writeInt(x -> x.numValues);

  private final String filenameBase;
  private final String offsetsFile;
  private final String blobsFile;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final CompressionStrategy compression;

  private int numValues;
  private long currentOffset;

  private CompressedLongsSerializer offsetsSerializer;
  private CompressedBlockSerializer valuesSerializer;

  public CompressedVariableSizedBlobColumnSerializer(
      final String filenameBase,
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final CompressionStrategy compression
  )
  {
    this.filenameBase = filenameBase;
    this.offsetsFile = getCompressedOffsetsFileName(filenameBase);
    this.blobsFile = getCompressedBlobsFileName(filenameBase);
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.compression = compression;
    this.numValues = 0;
  }

  public void open() throws IOException
  {
    numValues = 0;
    currentOffset = 0;
    offsetsSerializer = new CompressedLongsSerializer(segmentWriteOutMedium, compression);
    offsetsSerializer.open();

    valuesSerializer = new CompressedBlockSerializer(
        segmentWriteOutMedium,
        compression,
        CompressedPools.BUFFER_SIZE
    );
    valuesSerializer.open();
  }

  public void addValue(byte[] bytes) throws IOException
  {
    valuesSerializer.addValue(bytes);

    currentOffset += bytes.length;
    offsetsSerializer.add(currentOffset);
    numValues++;
    if (numValues < 0) {
      throw new ColumnCapacityExceededException(filenameBase);
    }
  }

  public void addValue(ByteBuffer bytes) throws IOException
  {
    currentOffset += bytes.remaining();
    valuesSerializer.addValue(bytes);
    offsetsSerializer.add(currentOffset);
    numValues++;
    if (numValues < 0) {
      throw new ColumnCapacityExceededException(filenameBase);
    }
  }

  @Override
  public long getSerializedSize()
  {
    // offsets and blobs stored in their own files, so our only size is metadata
    return META_SERDE_HELPER.size(this);
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    try (SmooshedWriter sub = smoosher.addWithSmooshedWriter(offsetsFile, offsetsSerializer.getSerializedSize())) {
      offsetsSerializer.writeTo(sub, smoosher);
    }
    try (SmooshedWriter sub = smoosher.addWithSmooshedWriter(blobsFile, valuesSerializer.getSerializedSize())) {
      valuesSerializer.writeTo(sub, smoosher);
    }
  }

  public static String getCompressedOffsetsFileName(String filenameBase)
  {
    return filenameBase + "_offsets";
  }

  public static String getCompressedBlobsFileName(String filenameBase)
  {
    return filenameBase + "_compressed";
  }
}
