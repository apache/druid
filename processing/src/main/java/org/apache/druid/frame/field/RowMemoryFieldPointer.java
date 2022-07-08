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
import org.apache.druid.frame.segment.row.ReadableFrameRowPointer;

/**
 * A {@link ReadableFieldPointer} that is derived from a row-based frame.
 */
public class RowMemoryFieldPointer implements ReadableFieldPointer
{
  private final Memory memory;
  private final ReadableFrameRowPointer rowPointer;
  private final int fieldNumber;
  private final int fieldCount;

  public RowMemoryFieldPointer(
      final Memory memory,
      final ReadableFrameRowPointer rowPointer,
      final int fieldNumber,
      final int fieldCount
  )
  {
    this.memory = memory;
    this.rowPointer = rowPointer;
    this.fieldNumber = fieldNumber;
    this.fieldCount = fieldCount;
  }

  @Override
  public long position()
  {
    if (fieldNumber == 0) {
      // First field starts after the field end pointers -- one integer per field.
      // (See RowReader javadocs for format details).
      return rowPointer.position() + (long) Integer.BYTES * fieldCount;
    } else {
      // Get position from the end pointer of the prior field.
      return rowPointer.position() + memory.getInt(rowPointer.position() + (long) Integer.BYTES * (fieldNumber - 1));
    }
  }
}
