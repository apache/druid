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
import org.apache.druid.frame.segment.row.ConstantFrameRowPointer;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for reading rows in the same format as used by {@link org.apache.druid.frame.FrameType#ROW_BASED}.
 *
 * Stateless and immutable.
 *
 * Row format:
 *
 * - 4 bytes * rowLength: field *end* pointers (exclusive), relative to the start of the row, little-endian ints
 * - N bytes * rowLength: fields written by {@link FieldWriter} implementations.
 */
public class RowReader
{
  private final List<FieldReader> fieldReaders;

  public RowReader(List<FieldReader> fieldReaders)
  {
    this.fieldReaders = fieldReaders;
  }

  public FieldReader fieldReader(final int fieldNumber)
  {
    return fieldReaders.get(fieldNumber);
  }

  public int fieldCount()
  {
    return fieldReaders.size();
  }

  /**
   * Read a particular field value as an object.
   *
   * For performance reasons, prefer {@link org.apache.druid.frame.read.FrameReader#makeCursorFactory}
   * for reading many rows out of a frame.
   */
  public Object readField(final Memory memory, final long rowPosition, final long rowLength, final int fieldNumber)
  {
    return fieldReaders.get(fieldNumber)
                       .makeColumnValueSelector(
                           memory,
                           new RowMemoryFieldPointer(
                               memory,
                               new ConstantFrameRowPointer(rowPosition, rowLength),
                               fieldNumber,
                               fieldReaders.size()
                           )
                       ).getObject();
  }

  /**
   * Read an entire row as a list of objects.
   *
   * For performance reasons, prefer {@link org.apache.druid.frame.read.FrameReader#makeCursorFactory}
   * for reading many rows out of a frame.
   */
  public List<Object> readRow(final Memory memory, final long rowPosition, final long rowLength)
  {
    final List<Object> objects = new ArrayList<>(fieldReaders.size());

    for (int i = 0; i < fieldReaders.size(); i++) {
      objects.add(readField(memory, rowPosition, rowLength, i));
    }

    return objects;
  }
}
