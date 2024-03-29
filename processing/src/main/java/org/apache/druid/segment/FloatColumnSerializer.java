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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.ColumnarFloatsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FloatColumnSerializer implements GenericColumnSerializer<Object>
{
  public static FloatColumnSerializer create(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      CompressionStrategy compression
  )
  {
    return new FloatColumnSerializer(columnName, segmentWriteOutMedium, filenameBase, IndexIO.BYTE_ORDER, compression);
  }

  private final String columnName;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressionStrategy compression;
  private ColumnarFloatsSerializer writer;

  private FloatColumnSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressionFactory.getFloatSerializer(
        columnName,
        segmentWriteOutMedium,
        StringUtils.format("%s.float_column", filenameBase),
        byteOrder,
        compression,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
  }

  @Override
  public void serialize(ColumnValueSelector<?> selector) throws IOException
  {
    writer.add(selector.getFloat());
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
