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

import com.google.common.math.LongMath;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.segment.column.ColumnType;

/**
 * Reader for columns written by {@link org.apache.druid.frame.write.columnar.DoubleArrayFrameColumnWriter}
 *
 * @see NumericArrayFrameColumnReader
 * @see org.apache.druid.frame.write.columnar.NumericArrayFrameColumnWriter for column's layout in memory
 */
public class DoubleArrayFrameColumnReader extends NumericArrayFrameColumnReader
{
  public DoubleArrayFrameColumnReader(int columnNumber)
  {
    super(FrameColumnWriters.TYPE_DOUBLE_ARRAY, ColumnType.DOUBLE_ARRAY, columnNumber);
  }

  @Override
  NumericArrayFrameColumn column(Frame frame, Memory memory, ColumnType columnType)
  {
    return new DoubleArrayFrameColumn(frame, memory, columnType);
  }

  private static class DoubleArrayFrameColumn extends NumericArrayFrameColumn
  {
    public DoubleArrayFrameColumn(
        Frame frame,
        Memory memory,
        ColumnType columnType
    )
    {
      super(frame, memory, columnType);
    }

    @Override
    Number getElement(Memory memory, long rowDataOffset, int cumulativeIndex)
    {
      return memory.getDouble(LongMath.checkedAdd(rowDataOffset, (long) cumulativeIndex * Double.BYTES));
    }
  }
}
