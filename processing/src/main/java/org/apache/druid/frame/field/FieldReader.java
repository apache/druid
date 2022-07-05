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
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;

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
   * Whether this field is comparable. Comparable fields can be compared as unsigned bytes.
   */
  boolean isComparable();
}
