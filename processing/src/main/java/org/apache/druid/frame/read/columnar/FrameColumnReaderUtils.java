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

import org.apache.datasketches.memory.Memory;

public class FrameColumnReaderUtils
{
  /**
   * Adjusts a negative cumulative row length from {@link #getCumulativeRowLength(Memory, long, int)} to be the actual
   * positive length.
   */
  public static int adjustCumulativeRowLength(final int cumulativeRowLength)
  {
    if (cumulativeRowLength < 0) {
      return -(cumulativeRowLength + 1);
    } else {
      return cumulativeRowLength;
    }
  }

  /**
   * Returns cumulative row length, if the row is not null itself, or -(cumulative row length) - 1 if the row is
   * null itself.
   *
   * To check if the return value from this function indicate a null row, use {@link #isNullRow(int)}
   *
   * To get the actual cumulative row length, use {@link FrameColumnReaderUtils#adjustCumulativeRowLength(int)}.
   */
  public static int getCumulativeRowLength(final Memory memory, final long offset, final int physicalRow)
  {
    // Note: only valid to call this if multiValue = true.
    return memory.getInt(offset + (long) Integer.BYTES * physicalRow);
  }

  public static int getAdjustedCumulativeRowLength(final Memory memory, final long offset, final int physicalRow)
  {
    return adjustCumulativeRowLength(getCumulativeRowLength(memory, offset, physicalRow));
  }

  /**
   * When given a return value from {@link FrameColumnReaderUtils#getCumulativeRowLength(Memory, long, int)}, returns whether the row is
   * null itself (i.e. a null array).
   */
  public static boolean isNullRow(final int cumulativeRowLength)
  {
    return cumulativeRowLength < 0;
  }
}
