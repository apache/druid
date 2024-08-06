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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reads values written by {@link FloatFieldWriter}.
 *
 * @see FloatFieldWriter
 * @see NumericFieldWriter for the details of the byte-format that it expects for reading
 */
public class FloatFieldReader extends NumericFieldReader
{

  public static FloatFieldReader forPrimitive()
  {
    return new FloatFieldReader(false);
  }

  public static FloatFieldReader forArray()
  {
    return new FloatFieldReader(true);
  }

  private FloatFieldReader(final boolean forArray)
  {
    super(forArray);
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.FLOAT;
  }

  @Override
  public ColumnValueSelector<?> getColumnValueSelector(
      final Memory memory,
      final ReadableFieldPointer fieldPointer,
      final byte nullIndicatorByte
  )
  {
    return new FloatFieldSelector(memory, fieldPointer, nullIndicatorByte);
  }

  private static class FloatFieldSelector extends NumericFieldReader.Selector implements FloatColumnSelector
  {

    final Memory dataRegion;
    final ReadableFieldPointer fieldPointer;

    public FloatFieldSelector(Memory dataRegion, ReadableFieldPointer fieldPointer, byte nullIndicatorByte)
    {
      super(dataRegion, fieldPointer, nullIndicatorByte);
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public float getFloat()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      final int bits = dataRegion.getInt(fieldPointer.position() + Byte.BYTES);
      return TransformUtils.detransformToFloat(bits);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public boolean isNull()
    {
      return super._isNull();
    }
  }

  @Override
  public Column makeRACColumn(Frame frame, RowSignature signature, String columnName)
  {
    return new FloatFieldReaderColumn(frame, signature.indexOf(columnName), signature.size());
  }

  private class FloatFieldReaderColumn implements Column
  {
    private final Frame frame;
    private final Memory dataRegion;
    private final FieldPositionHelper coach;

    public FloatFieldReaderColumn(Frame frame, int columnIndex, int numFields)
    {
      this.frame = frame;
      dataRegion = frame.region(RowBasedFrameWriter.ROW_DATA_REGION);

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
      return new ColumnAccessor()
      {
        @Override
        public ColumnType getType()
        {
          return ColumnType.FLOAT;
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
          return dataRegion.getByte(fieldPosition) == getNullIndicatorByte();
        }

        @Nullable
        @Override
        public Object getObject(int rowNum)
        {
          final long fieldPosition = coach.computeFieldPosition(rowNum);

          if (dataRegion.getByte(fieldPosition) == getNullIndicatorByte()) {
            return null;
          } else {
            return getFloatAtPosition(fieldPosition);
          }
        }

        @Override
        public double getDouble(int rowNum)
        {
          return getFloat(rowNum);
        }

        @Override
        public float getFloat(int rowNum)
        {
          final long fieldPosition = coach.computeFieldPosition(rowNum);

          if (dataRegion.getByte(fieldPosition) == getNullIndicatorByte()) {
            return 0L;
          } else {
            return getFloatAtPosition(fieldPosition);
          }
        }

        @Override
        public long getLong(int rowNum)
        {
          return (long) getFloat(rowNum);
        }

        @Override
        public int getInt(int rowNum)
        {
          return (int) getFloat(rowNum);
        }

        @Override
        public int compareRows(int lhsRowNum, int rhsRowNum)
        {
          long lhsPosition = coach.computeFieldPosition(lhsRowNum);
          long rhsPosition = coach.computeFieldPosition(rhsRowNum);

          final byte nullIndicatorByte = getNullIndicatorByte();
          if (dataRegion.getByte(lhsPosition) == nullIndicatorByte) {
            if (dataRegion.getByte(rhsPosition) == nullIndicatorByte) {
              return 0;
            } else {
              return -1;
            }
          } else {
            if (dataRegion.getByte(rhsPosition) == nullIndicatorByte) {
              return 1;
            } else {
              return Float.compare(getFloatAtPosition(lhsPosition), getFloatAtPosition(rhsPosition));
            }
          }
        }

        private float getFloatAtPosition(long rhsPosition)
        {
          return TransformUtils.detransformToFloat(dataRegion.getInt(rhsPosition + Byte.BYTES));
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
