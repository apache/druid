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

package org.apache.druid.segment.serde;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSerializer;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedComplexColumnSerializer<T> implements GenericColumnSerializer<T>
{
  public static final byte IS_COMPRESSED = Byte.MAX_VALUE;
  public static final byte V0 = 0x00;
  public static final String FILE_NAME = "__complexColumn";

  public static GenericColumnSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String column,
      IndexSpec indexSpec,
      ObjectStrategy objectStrategy
  )
  {
    return new CompressedComplexColumnSerializer(column, segmentWriteOutMedium, indexSpec, objectStrategy);
  }

  public CompressedComplexColumnSerializer(
      String name,
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec,
      ObjectStrategy<T> strategy
  )
  {
    this.name = name;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.strategy = strategy;
  }

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  private final ObjectStrategy<T> strategy;
  private CompressedVariableSizedBlobColumnSerializer writer;
  private ByteBufferWriter<ImmutableBitmap> nullBitmapWriter;
  private MutableBitmap nullRowsBitmap;

  private int rowCount = 0;
  private boolean closedForWrite = false;
  private byte[] metadataBytes;

  @Override
  public void open() throws IOException
  {
    writer = new CompressedVariableSizedBlobColumnSerializer(
        ColumnSerializerUtils.getInternalFileName(name, FILE_NAME),
        segmentWriteOutMedium,
        indexSpec.getComplexMetricCompression()
    );
    writer.open();

    nullBitmapWriter = new ByteBufferWriter<>(
        segmentWriteOutMedium,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    nullBitmapWriter.open();

    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends T> selector) throws IOException
  {
    final T data = selector.getObject();
    if (data == null) {
      nullRowsBitmap.add(rowCount);
    }
    rowCount++;
    final byte[] bytes = strategy.toBytes(data);
    writer.addValue(bytes);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    closeForWrite();
    // COMPRESSED_BYTE + V0 + metadata
    return 1 + 1 + metadataBytes.length;
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(closedForWrite, "Not closed yet!");

    channel.write(ByteBuffer.wrap(new byte[]{IS_COMPRESSED}));
    channel.write(ByteBuffer.wrap(new byte[]{V0}));
    channel.write(ByteBuffer.wrap(metadataBytes));

    ColumnSerializerUtils.writeInternal(smoosher, writer, name, FILE_NAME);
    if (!nullRowsBitmap.isEmpty()) {
      ColumnSerializerUtils.writeInternal(
          smoosher,
          nullBitmapWriter,
          name,
          ColumnSerializerUtils.NULL_BITMAP_FILE_NAME
      );
    }
  }

  private void closeForWrite() throws IOException
  {
    if (!closedForWrite) {
      closedForWrite = true;
      nullBitmapWriter.write(nullRowsBitmap);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      IndexMerger.SERIALIZER_UTILS.writeString(
          baos,
          ColumnSerializerUtils.SMILE_MAPPER.writeValueAsString(
              new ComplexColumnMetadata(
                  ByteOrder.nativeOrder(),
                  indexSpec.getBitmapSerdeFactory(),
                  name,
                  !nullRowsBitmap.isEmpty()
              )
          )
      );
      this.metadataBytes = baos.toByteArray();
    }
  }
}
