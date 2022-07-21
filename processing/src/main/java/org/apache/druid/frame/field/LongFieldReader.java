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
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

/**
 * Reads values written by {@link LongFieldWriter}.
 *
 * Values are sortable as bytes without decoding.
 *
 * Format:
 *
 * - 1 byte: {@link LongFieldWriter#NULL_BYTE} or {@link LongFieldWriter#NOT_NULL_BYTE}
 * - 8 bytes: encoded long: big-endian order, with sign flipped
 */
public class LongFieldReader implements FieldReader
{
  LongFieldReader()
  {
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return new Selector(memory, fieldPointer);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    return ValueTypes.makeNumericWrappingDimensionSelector(
        ValueType.LONG,
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  /**
   * Selector that reads a value from a location pointed to by {@link ReadableFieldPointer}.
   */
  private static class Selector implements LongColumnSelector
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory memory, final ReadableFieldPointer fieldPointer)
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public long getLong()
    {
      assert !isNull();
      final long bits = memory.getLong(fieldPointer.position() + Byte.BYTES);
      return Long.reverseBytes(bits) ^ Long.MIN_VALUE;
    }

    @Override
    public boolean isNull()
    {
      return memory.getByte(fieldPointer.position()) == LongFieldWriter.NULL_BYTE;
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
