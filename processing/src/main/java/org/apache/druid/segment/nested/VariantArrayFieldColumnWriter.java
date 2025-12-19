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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

/**
 * Nested field writer for array type columns of {@link NestedDataColumnSerializer}. This is currently only used
 * for single type array columns, but can handle any type of array. {@link VariantFieldColumnWriter} is used for mixed
 */
public class VariantArrayFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<int[]>
{

  public VariantArrayFieldColumnWriter(
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
  int[] processValue(int row, Object value)
  {
    // replace Object[] with int[] composed of the global ids
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
        arrayElements.computeIfAbsent(
            globalIds[i],
            (id) -> columnFormatSpec.getBitmapEncoding().getBitmapFactory().makeEmptyMutableBitmap()
        ).add(row);
      }
      return globalIds;
    }
    if (value != null) {
      // this writer is only used for arrays so all values will have been coerced to Object[] at this point
      throw new ISE("Value is not an array, got [%s] instead", value.getClass());
    }
    return null;
  }

  @Override
  int lookupGlobalId(int[] value)
  {
    return globalDictionaryIdLookup.lookupArray(value);
  }
}
