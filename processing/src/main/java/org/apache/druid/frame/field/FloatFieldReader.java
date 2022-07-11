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
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

/**
 * Reads values written by {@link FloatFieldWriter}.
 *
 * Values are sortable as bytes without decoding.
 *
 * Format:
 *
 * - 1 byte: {@link FloatFieldWriter#NULL_BYTE} or {@link FloatFieldWriter#NOT_NULL_BYTE}
 * - 4 bytes: encoded float, using {@link FloatFieldWriter#transform}
 */
public class FloatFieldReader implements FieldReader
{
  FloatFieldReader()
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
        ValueType.FLOAT,
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  private static class Selector implements FloatColumnSelector
  {
    private final Memory dataRegion;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory dataRegion, final ReadableFieldPointer fieldPointer)
    {
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public float getFloat()
    {
      assert !isNull();
      final int bits = dataRegion.getInt(fieldPointer.position() + Byte.BYTES);
      return FloatFieldWriter.detransform(bits);
    }

    @Override
    public boolean isNull()
    {
      return dataRegion.getByte(fieldPointer.position()) == FloatFieldWriter.NULL_BYTE;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
