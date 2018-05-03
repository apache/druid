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

package io.druid.segment;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ColumnarDoublesSerializer;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DoubleColumnSerializer implements GenericColumnSerializer
{
  public static DoubleColumnSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      CompressionStrategy compression
  )
  {
    return new DoubleColumnSerializer(segmentWriteOutMedium, filenameBase, IndexIO.BYTE_ORDER, compression);
  }

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressionStrategy compression;
  private ColumnarDoublesSerializer writer;

  private DoubleColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressionFactory.getDoubleSerializer(
        segmentWriteOutMedium,
        StringUtils.format("%s.double_column", filenameBase),
        byteOrder,
        compression
    );
    writer.open();
  }

  @Override
  public void serialize(ColumnValueSelector selector) throws IOException
  {
    writer.add(selector.getDouble());
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writer.writeTo(channel, smoosher);
  }

}
