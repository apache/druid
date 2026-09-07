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

package org.apache.druid.segment.nested;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.data.ColumnarLongsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Nested field writer for long type columns of {@link NestedDataColumnSerializer}. In addition to the normal
 * dictionary encoded column, this writer also writes an additional long value column with {@link #longsSerializer},
 * which is written to during {@link #addValue}.
 */
public final class ScalarLongFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Long>
{
  private ColumnarLongsSerializer longsSerializer;

  public ScalarLongFieldColumnWriter(
      String columnName,
      String fieldName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      NestedCommonFormatColumnFormatSpec columnFormatSpec,
      DictionaryIdLookup globalDictionaryIdLookup
  )
  {
    super(columnName, fieldName, segmentWriteOutMedium, columnFormatSpec, globalDictionaryIdLookup);
    bitmapIndexType = columnFormatSpec.getLongFieldBitmapIndexType();
  }

  @Override
  int lookupGlobalId(Long value)
  {
    return globalDictionaryIdLookup.lookupLong(value);
  }

  @Override
  void writeValue(@Nullable Long value) throws IOException
  {
    if (value == null) {
      longsSerializer.add(0L);
    } else {
      longsSerializer.add(value);
    }
  }

  @Override
  public void openColumnSerializer(SegmentWriteOutMedium medium, int maxId) throws IOException
  {
    super.openColumnSerializer(medium, maxId);
    longsSerializer = CompressionFactory.getLongSerializer(
        fieldName,
        medium,
        StringUtils.format("%s.long_column", fieldName),
        ByteOrder.nativeOrder(),
        columnFormatSpec.getLongColumnEncoding(),
        columnFormatSpec.getLongColumnCompression(),
        fieldResourceCloser
    );
    longsSerializer.open();
  }

  @Override
  void writeColumnTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
  {
    writeLongAndDoubleColumnLength(channel, Ints.checkedCast(longsSerializer.getSerializedSize()), 0);
    longsSerializer.writeTo(channel, fileBuilder);
    encodedValueSerializer.writeTo(channel, fileBuilder);
  }

  @Override
  long getSerializedColumnSize() throws IOException
  {
    return Integer.BYTES + Integer.BYTES
           + longsSerializer.getSerializedSize()
           + encodedValueSerializer.getSerializedSize();
  }
}
