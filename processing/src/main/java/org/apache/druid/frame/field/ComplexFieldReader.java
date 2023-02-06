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

import com.google.common.base.Preconditions;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;

/**
 * Reads values written by {@link ComplexFieldWriter}.
 *
 * Format:
 *
 * - 1 byte: {@link ComplexFieldWriter#NULL_BYTE} or {@link ComplexFieldWriter#NOT_NULL_BYTE}
 * - 4 bytes: length of serialized complex value, little-endian int
 * - N bytes: serialized complex value
 */
public class ComplexFieldReader implements FieldReader
{
  private final ComplexMetricSerde serde;

  ComplexFieldReader(final ComplexMetricSerde serde)
  {
    this.serde = Preconditions.checkNotNull(serde, "serde");
  }

  public static ComplexFieldReader createFromType(final ColumnType columnType)
  {
    if (columnType == null || columnType.getType() != ValueType.COMPLEX || columnType.getComplexTypeName() == null) {
      throw new ISE("Expected complex type with defined complexTypeName, but got [%s]", columnType);
    }

    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(columnType.getComplexTypeName());

    if (serde == null) {
      throw new ISE("No serde for complexTypeName[%s]", columnType.getComplexTypeName());
    }

    return new ComplexFieldReader(serde);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return new Selector<>(memory, fieldPointer, serde);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    return DimensionSelector.constant(null, extractionFn);
  }

  @Override
  public boolean isComparable()
  {
    return false;
  }

  /**
   * Selector that reads a value from a location pointed to by {@link ReadableFieldPointer}.
   */
  private static class Selector<T> extends ObjectColumnSelector<T>
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;
    private final ComplexMetricSerde serde;

    private Selector(Memory memory, ReadableFieldPointer fieldPointer, ComplexMetricSerde serde)
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
      this.serde = serde;
    }

    @Nullable
    @Override
    public T getObject()
    {
      final long fieldPosition = fieldPointer.position();
      final byte nullByte = memory.getByte(fieldPosition);

      if (nullByte == ComplexFieldWriter.NULL_BYTE) {
        return null;
      } else if (nullByte == ComplexFieldWriter.NOT_NULL_BYTE) {
        final int length = memory.getInt(fieldPosition + Byte.BYTES);
        final byte[] bytes = new byte[length];
        memory.getByteArray(fieldPosition + ComplexFieldWriter.HEADER_SIZE, bytes, 0, length);

        //noinspection unchecked
        return (T) serde.fromBytes(bytes, 0, length);
      } else {
        throw new ISE("Unexpected null byte [%s]", nullByte);
      }
    }

    @Override
    public Class<T> classOfObject()
    {
      return serde.getExtractor().extractedClass();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
