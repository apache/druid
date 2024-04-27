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

package org.apache.druid.spectator.histogram;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.data.ColumnCapacityExceededException;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class SpectatorHistogramSerializer implements GenericColumnSerializer<Object>
{
  private static final MetaSerdeHelper<SpectatorHistogramSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((SpectatorHistogramSerializer x) -> SpectatorHistogramIndexed.VERSION_ONE)
      .writeByte(x -> SpectatorHistogramIndexed.RESERVED_FLAGS)
      // numBytesUsed field is header + values (i.e. all bytes _after_ this)
      .writeInt(x -> Ints.checkedCast(x.offsetsHeader.getSerializedSize() + x.valuesBuffer.size()));

  public static SpectatorHistogramSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      ObjectStrategy<SpectatorHistogram> strategy
  )
  {
    return new SpectatorHistogramSerializer(
        columnName,
        segmentWriteOutMedium,
        strategy
    );
  }

  private final String columnName;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ObjectStrategy<SpectatorHistogram> objectStrategy;
  private NullableOffsetsHeader offsetsHeader;
  private WriteOutBytes valuesBuffer;

  private int rowCount = 0;

  private SpectatorHistogramSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ObjectStrategy<SpectatorHistogram> strategy
  )
  {
    this.columnName = columnName;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.objectStrategy = strategy;
  }

  @Override
  public void open() throws IOException
  {
    this.offsetsHeader = NullableOffsetsHeader.create(segmentWriteOutMedium);
    this.valuesBuffer = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void serialize(ColumnValueSelector<?> selector) throws IOException
  {
    rowCount++;
    if (rowCount < 0) {
      throw new ColumnCapacityExceededException(columnName);
    }
    Object value = selector.getObject();
    if (value == null) {
      offsetsHeader.writeNull();
    } else {
      objectStrategy.writeTo((SpectatorHistogram) value, valuesBuffer);
      offsetsHeader.writeOffset(Ints.checkedCast(valuesBuffer.size()));
    }
  }

  @Override
  public long getSerializedSize()
  {
    // Meta header, Offsets, Values
    return META_SERDE_HELPER.size(this) + offsetsHeader.getSerializedSize() + valuesBuffer.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    offsetsHeader.writeTo(channel, null);
    valuesBuffer.writeTo(channel);
  }
}
