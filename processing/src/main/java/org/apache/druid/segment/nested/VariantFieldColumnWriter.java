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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Nested field writer for mixed type scalar or array columns of {@link NestedDataColumnSerializer}.
 */
public final class VariantFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Object>
{
  public VariantFieldColumnWriter(
      String columnName,
      String fieldName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec,
      DictionaryIdLookup globalDictionaryIdLookup
  )
  {
    super(columnName, fieldName, segmentWriteOutMedium, indexSpec, globalDictionaryIdLookup);
  }


  @Override
  Object processValue(int row, Object value)
  {
    // replace arrays represented as Object[] with int[] composed of the global ids
    if (value instanceof Object[]) {
      Object[] array = (Object[]) value;
      final int[] globalIds = new int[array.length];
      for (int i = 0; i < array.length; i++) {
        if (array[i] == null) {
          globalIds[i] = 0;
        } else if (array[i] instanceof String) {
          globalIds[i] = globalDictionaryIdLookup.lookupString((String) array[i]);
        } else if (array[i] instanceof Long) {
          globalIds[i] = globalDictionaryIdLookup.lookupLong((Long) array[i]);
        } else if (array[i] instanceof Double) {
          globalIds[i] = globalDictionaryIdLookup.lookupDouble((Double) array[i]);
        } else {
          globalIds[i] = -1;
        }
        Preconditions.checkArgument(globalIds[i] >= 0, "unknown global id [%s] for value [%s]", globalIds[i], array[i]);
      }
      return globalIds;
    }
    // non-arrays can be passed directly through
    return super.processValue(row, value);
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
    } else if (value instanceof int[]) {
      return globalDictionaryIdLookup.lookupArray((int[]) value);
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
