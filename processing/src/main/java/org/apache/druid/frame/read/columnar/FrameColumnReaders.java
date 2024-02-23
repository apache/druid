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

package org.apache.druid.frame.read.columnar;

import org.apache.druid.segment.column.ColumnType;

/**
 * Creates {@link FrameColumnReader}  corresponding to a given column type and number.
 *
 * Returns a dummy {@link UnsupportedColumnTypeFrameColumnReader} if the column type is not supported or unknown.
 * Calling any method of the dummy reader will throw with relevant error message.
 */
public class FrameColumnReaders
{
  private FrameColumnReaders()
  {
    // No instantiation.
  }

  public static FrameColumnReader create(
      final String columnName,
      final int columnNumber,
      final ColumnType columnType
  )
  {
    switch (columnType.getType()) {
      case LONG:
        return new LongFrameColumnReader(columnNumber);

      case FLOAT:
        return new FloatFrameColumnReader(columnNumber);

      case DOUBLE:
        return new DoubleFrameColumnReader(columnNumber);

      case STRING:
        return new StringFrameColumnReader(columnNumber, false);

      case COMPLEX:
        return new ComplexFrameColumnReader(columnNumber);

      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
            return new StringFrameColumnReader(columnNumber, true);
          case LONG:
            return new LongArrayFrameColumnReader(columnNumber);
          case FLOAT:
            return new FloatArrayFrameColumnReader(columnNumber);
          case DOUBLE:
            return new DoubleArrayFrameColumnReader(columnNumber);
          default:
            return new UnsupportedColumnTypeFrameColumnReader(columnName, columnType);
        }
      default:
        return new UnsupportedColumnTypeFrameColumnReader(columnName, columnType);
    }
  }
}
