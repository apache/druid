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

import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;

/**
 * Helps compute the field position for a frame from the different regions in the frame.
 */
public class FieldPositionHelper
{
  private final Frame frame;
  private final Memory offsetRegion;
  private final Memory dataRegion;
  private final int columnIndex;
  private final long fieldsBytesSize;

  public FieldPositionHelper(
      Frame frame,
      Memory offsetRegion,
      Memory dataRegion,
      int columnIndex,
      int numFields
  )
  {
    this.frame = frame;
    this.offsetRegion = offsetRegion;
    this.dataRegion = dataRegion;
    this.columnIndex = columnIndex;
    this.fieldsBytesSize = this.columnIndex == 0
                           ? ((long) numFields) * Integer.BYTES
                           : ((long) (this.columnIndex - 1)) * Integer.BYTES;
  }

  public long computeFieldPosition(int rowNum)
  {
    rowNum = frame.physicalRow(rowNum);
    final long rowPosition = rowNum == 0 ? 0 : offsetRegion.getLong(((long) rowNum - 1) * Long.BYTES);
    final long fieldPosition;
    if (columnIndex == 0) {
      fieldPosition = rowPosition + fieldsBytesSize;
    } else {
      fieldPosition = rowPosition + dataRegion.getInt(rowPosition + fieldsBytesSize);
    }
    return fieldPosition;
  }
}
