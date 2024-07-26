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

package org.apache.druid.frame.field;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.segment.column.ColumnType;

/**
 * Helper used to read field values from row-based frames or {@link RowKey}.
 *
 * Most callers should use {@link org.apache.druid.frame.read.FrameReader} or {@link RowKeyReader} rather than using
 * this class directly.
 */
public class FieldReaders
{
  private FieldReaders()
  {
    // No instantiation.
  }

  /**
   * Helper used by {@link org.apache.druid.frame.read.FrameReader}.
   */
  public static FieldReader create(final String columnName, final ColumnType columnType)
  {
    switch (Preconditions.checkNotNull(columnType, "columnType").getType()) {
      case LONG:
        return LongFieldReader.forPrimitive();

      case FLOAT:
        return FloatFieldReader.forPrimitive();

      case DOUBLE:
        return DoubleFieldReader.forPrimitive();

      case STRING:
        return new StringFieldReader();

      case COMPLEX:
        return ComplexFieldReader.createFromType(columnType);

      case ARRAY:
        switch (Preconditions.checkNotNull(columnType.getElementType().getType(), "array elementType")) {
          case STRING:
            return new StringArrayFieldReader();

          case LONG:
            return new LongArrayFieldReader();

          case FLOAT:
            return new FloatArrayFieldReader();

          case DOUBLE:
            return new DoubleArrayFieldReader();

          default:
            throw new UnsupportedColumnTypeException(columnName, columnType);

        }
        // Fall through to error for other array types

      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
  }
}
