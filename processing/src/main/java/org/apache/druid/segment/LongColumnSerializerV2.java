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

import com.google.common.primitives.Ints;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.ColumnarLongsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Column Serializer for long column.
 * The column is serialized in two parts, first a bitmap indicating the nullability of row values
 * and second the actual row values.
 * This class is unsafe for concurrent use from multiple threads.
 */
public class LongColumnSerializerV2 implements GenericColumnSerializer<Object>
{
  public static LongColumnSerializerV2 create(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      CompressionStrategy compression,
      CompressionFactory.LongEncodingStrategy encoding,
      BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new LongColumnSerializerV2(
        columnName,
        segmentWriteOutMedium,
        filenameBase,
        IndexIO.BYTE_ORDER,
        compression,
        encoding,
        bitmapSerdeFactory
    );
  }

  private final String columnName;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressionStrategy compression;
  private final CompressionFactory.LongEncodingStrategy encoding;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  private ColumnarLongsSerializer writer;
  private ByteBufferWriter<ImmutableBitmap> nullValueBitmapWriter;
  private MutableBitmap nullRowsBitmap;
  private int rowCount = 0;

  private LongColumnSerializerV2(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression,
      CompressionFactory.LongEncodingStrategy encoding,
      BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    this.columnName = columnName;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.encoding = encoding;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressionFactory.getLongSerializer(
        columnName,
        segmentWriteOutMedium,
        StringUtils.format("%s.long_column", filenameBase),
        byteOrder,
        encoding,
        compression
    );
    writer.open();
    nullValueBitmapWriter = new ByteBufferWriter<>(
        segmentWriteOutMedium,
        bitmapSerdeFactory.getObjectStrategy()
    );
    nullValueBitmapWriter.open();
    nullRowsBitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
  }

  @Override
  public void serialize(ColumnValueSelector<?> selector) throws IOException
  {
    if (selector.isNull()) {
      nullRowsBitmap.add(rowCount);
      writer.add(0L);
    } else {
      writer.add(selector.getLong());
    }
    rowCount++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    nullValueBitmapWriter.write(bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(nullRowsBitmap));
    long bitmapSize = nullRowsBitmap.isEmpty()
                      ? 0L
                      : nullValueBitmapWriter.getSerializedSize();
    return Integer.BYTES + writer.getSerializedSize() + bitmapSize;
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    SerializerUtils.writeInt(channel, Ints.checkedCast(writer.getSerializedSize()));
    writer.writeTo(channel, smoosher);
    if (!nullRowsBitmap.isEmpty()) {
      nullValueBitmapWriter.writeTo(channel, smoosher);
    }
  }
}
