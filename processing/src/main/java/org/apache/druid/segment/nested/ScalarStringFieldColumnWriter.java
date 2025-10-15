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

import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Nested field writer for string type columns of {@link NestedDataColumnSerializer}
 */
public final class ScalarStringFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<String>
{
  public ScalarStringFieldColumnWriter(
      String columnName,
      String fieldName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      NestedCommonFormatColumnFormatSpec columnFormatSpec,
      DictionaryIdLookup globalDictionaryIdLookup
  )
  {
    super(columnName, fieldName, segmentWriteOutMedium, columnFormatSpec, globalDictionaryIdLookup);
  }

  @Override
  String processValue(int row, Object value)
  {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  @Override
  int lookupGlobalId(String value)
  {
    return globalDictionaryIdLookup.lookupString(value);
  }

  @Override
  void writeColumnTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
  {
    writeLongAndDoubleColumnLength(channel, 0, 0);
    encodedValueSerializer.writeTo(channel, fileBuilder);
  }
}
