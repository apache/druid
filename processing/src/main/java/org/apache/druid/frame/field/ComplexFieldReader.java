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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.accessor.ObjectColumnAccessorBase;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * Reads values written by {@link ComplexFieldWriter}.
 * <p>
 * Format:
 * <p>
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
  public boolean isNull(Memory memory, long position)
  {
    return memory.getByte(position) == ComplexFieldWriter.NULL_BYTE;
  }

  /**
   * Alternative interface to read the field from the byte array without creating a selector and field pointer. It is much
   * faster than wrapping the byte array in Memory for reading.
   */
  @Nullable
  public static Object readFieldFromByteArray(
      final ComplexMetricSerde serde,
      final byte[] bytes,
      final int position
  )
  {
    final byte nullByte = bytes[position];

    if (nullByte == ComplexFieldWriter.NULL_BYTE) {
      return null;
    } else if (nullByte == ComplexFieldWriter.NOT_NULL_BYTE) {
      // Reads length in little-endian format
      int length;
      length = (bytes[position + 4] & 0xFF) << 24;
      length |= (bytes[position + 3] & 0xFF) << 16;
      length |= (bytes[position + 2] & 0xFF) << 8;
      length |= (bytes[position + 1] & 0xFF);
      return serde.fromBytes(bytes, position + ComplexFieldWriter.HEADER_SIZE, length);
    } else {
      throw new ISE("Unexpected null byte [%s]", nullByte);
    }
  }

  /**
   * Alternative interface to read the field from the memory without creating a selector and field pointer
   */
  @Nullable
  public static <T> T readFieldFromMemory(
      final ComplexMetricSerde serde,
      final Memory memory,
      final long position
  )
  {
    final byte nullByte = memory.getByte(position);

    if (nullByte == ComplexFieldWriter.NULL_BYTE) {
      return null;
    } else if (nullByte == ComplexFieldWriter.NOT_NULL_BYTE) {
      final int length = memory.getInt(position + Byte.BYTES);
      final byte[] bytes = new byte[length];
      memory.getByteArray(position + ComplexFieldWriter.HEADER_SIZE, bytes, 0, length);

      //noinspection unchecked
      return (T) serde.fromBytes(bytes, 0, length);
    } else {
      throw new ISE("Unexpected null byte [%s]", nullByte);
    }
  }

  /**
   * Selector that reads a value from a location pointed to by {@link ReadableFieldPointer}.
   */
  private static class Selector<T> extends ObjectColumnSelector<T>
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;
    private final ComplexMetricSerde serde;
    @SuppressWarnings("rawtypes")
    private final Class clazz;

    private Selector(Memory memory, ReadableFieldPointer fieldPointer, ComplexMetricSerde serde)
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
      this.serde = serde;
      //noinspection deprecation
      this.clazz = serde.getObjectStrategy().getClazz();
    }

    @Nullable
    @Override
    public T getObject()
    {
      final long fieldPosition = fieldPointer.position();
      return readFieldFromMemory(serde, memory, fieldPosition);
    }

    @Override
    public Class<T> classOfObject()
    {
      //noinspection unchecked
      return clazz;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }

  @Override
  public Column makeRACColumn(Frame frame, RowSignature signature, String columnName)
  {
    return new ComplexFieldReaderColumn(frame, signature.indexOf(columnName), signature.size());
  }

  private class ComplexFieldReaderColumn implements Column
  {
    private final Frame frame;
    private final Memory dataRegion;
    private final ColumnType type;
    private final FieldPositionHelper coach;

    public ComplexFieldReaderColumn(Frame frame, int columnIndex, int numFields)
    {
      this.frame = frame;
      dataRegion = frame.region(RowBasedFrameWriter.ROW_DATA_REGION);

      this.type = ColumnType.ofComplex(serde.getTypeName());
      this.coach = new FieldPositionHelper(
          frame,
          frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION),
          dataRegion,
          columnIndex,
          numFields
      );
    }

    @Nonnull
    @Override
    public ColumnAccessor toAccessor()
    {
      return new ObjectColumnAccessorBase()
      {
        @Override
        public ColumnType getType()
        {
          return type;
        }

        @Override
        public int numRows()
        {
          return frame.numRows();
        }

        @Override
        public boolean isNull(int rowNum)
        {
          final long fieldPosition = coach.computeFieldPosition(rowNum);
          return dataRegion.getByte(fieldPosition) == ComplexFieldWriter.NULL_BYTE;
        }

        @Override
        protected Object getVal(int rowNum)
        {
          return readFieldFromMemory(serde, dataRegion, coach.computeFieldPosition(rowNum));
        }

        @Override
        protected Comparator<Object> getComparator()
        {
          return serde.getTypeStrategy();
        }

      };
    }

    @Nullable
    @Override
    public <T> T as(Class<? extends T> clazz)
    {
      return null;
    }
  }
}
