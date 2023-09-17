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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.read.ColumnPlus;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

/**
 * Embeds the logic to read a specific field from row-based frames or from {@link RowKey}.
 *
 * Most callers should use {@link org.apache.druid.frame.read.FrameReader} or
 * {@link RowKeyReader} rather than using this interface directly.
 *
 * Stateless and immutable.
 */
public interface FieldReader
{

  int ILLEGAL_FIELD_NUMBER = -1;
  int ILLEGAL_FIELD_COUNT = -1;

  static ColumnCapabilities.Capable hasNullsForFieldReader()
  {
    if (NullHandling.replaceWithDefault()) {
      return ColumnCapabilities.Capable.FALSE;
    }
    return ColumnCapabilities.Capable.UNKNOWN;
  }

  /**
   * Create a {@link ColumnValueSelector} backed by some memory and a moveable pointer.
   */
  ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer);

  /**
   * Create a {@link DimensionSelector} backed by some memory and a moveable pointer.
   */
  DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  );

  /**
   * Whether the provided memory position points to a null value.
   */
  boolean isNull(Memory memory, long position);

  /**
   * Whether this field is comparable. Comparable fields can be compared as unsigned bytes.
   */
  boolean isComparable();

  /**
   * Returns a column reference for the provided frame. The performance of this is likely worse than the one provided
   * by the COLUMNAR based frames. Therefore, if the primary task revolves around referencing and reading a column, it
   * is better to use {@link org.apache.druid.frame.read.columnar.FrameColumnReader#readColumn} instead of ROW_BASED
   * frames. Used to test the behaviour of joins in the LHS
   */
  default ColumnPlus readColumn(Frame frame)
  {
    throw DruidException.defensive("Cannot call readColumn() on the FieldReader[%s]", this.getClass());
  }
}
