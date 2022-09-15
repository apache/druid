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
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.ColumnarDoublesSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Literal field writer for double type nested columns of {@link NestedDataColumnSerializer}. In addition to the normal
 * dictionary encoded column, this writer also writes an additional double value column with {@link #doublesSerializer},
 * which is written to during {@link #addValue}.
 */
public final class DoubleFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Double>
{
  private ColumnarDoublesSerializer doublesSerializer;

  protected DoubleFieldColumnWriter(
      String columnName,
      String fieldName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec,
      GlobalDictionaryIdLookup globalDictionaryIdLookup
  )
  {
    super(columnName, fieldName, segmentWriteOutMedium, indexSpec, globalDictionaryIdLookup);
  }

  @Override
  int lookupGlobalId(Double value)
  {
    return globalDictionaryIdLookup.lookupDouble(value);
  }

  @Override
  public void open() throws IOException
  {
    super.open();
    doublesSerializer = CompressionFactory.getDoubleSerializer(
        fieldName,
        segmentWriteOutMedium,
        StringUtils.format("%s.double_column", fieldName),
        ByteOrder.nativeOrder(),
        indexSpec.getDimensionCompression()
    );
    doublesSerializer.open();
  }

  @Override
  void writeValue(@Nullable Double value) throws IOException
  {
    if (value == null) {
      doublesSerializer.add(0.0);
    } else {
      doublesSerializer.add(value);
    }
  }

  @Override
  void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeLongAndDoubleColumnLength(channel, 0, Ints.checkedCast(doublesSerializer.getSerializedSize()));
    doublesSerializer.writeTo(channel, smoosher);
    encodedValueSerializer.writeTo(channel, smoosher);
  }

  @Override
  long getSerializedColumnSize() throws IOException
  {
    return super.getSerializedColumnSize() + doublesSerializer.getSerializedSize();
  }
}
