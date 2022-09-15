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

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Literal field writer for mixed type nested columns of {@link NestedDataColumnSerializer}
 */
public final class VariantLiteralFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Object>
{
  public VariantLiteralFieldColumnWriter(
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
  int lookupGlobalId(Object value)
  {
    if (value == null) {
      return 0;
    }
    if (value instanceof Long) {
      return globalDictionaryIdLookup.lookupLong((Long) value);
    } else if (value instanceof Double) {
      return globalDictionaryIdLookup.lookupDouble((Double) value);
    } else {
      return globalDictionaryIdLookup.lookupString(String.valueOf(value));
    }
  }

  @Override
  void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeLongAndDoubleColumnLength(channel, 0, 0);
    encodedValueSerializer.writeTo(channel, smoosher);
  }
}
