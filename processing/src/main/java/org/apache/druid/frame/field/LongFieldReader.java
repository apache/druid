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
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reads values written by {@link LongFieldWriter}.
 *
 * @see LongFieldWriter
 * @see NumericFieldWriter for the details of the byte-format that it expects for reading
 */
public class LongFieldReader extends NumericFieldReader
{

  public static LongFieldReader forPrimitive()
  {
    return new LongFieldReader(false);
  }

  public static LongFieldReader forArray()
  {
    return new LongFieldReader(true);
  }

  private LongFieldReader(final boolean forArray)
  {
    super(forArray);
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.LONG;
  }

  @Override
  public ColumnValueSelector<?> getColumnValueSelector(
      final Memory memory,
      final ReadableFieldPointer fieldPointer,
      final byte nullIndicatorByte
  )
  {
    return new LongFieldSelector(memory, fieldPointer, nullIndicatorByte);
  }

  private static class LongFieldSelector extends NumericFieldReader.Selector implements LongColumnSelector
  {
    final Memory dataRegion;
    final ReadableFieldPointer fieldPointer;

    public LongFieldSelector(Memory dataRegion, ReadableFieldPointer fieldPointer, byte nullIndicatorByte)
    {
      super(dataRegion, fieldPointer, nullIndicatorByte);
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public long getLong()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      final long bits = dataRegion.getLong(fieldPointer.position() + Byte.BYTES);
      return TransformUtils.detransformToLong(bits);
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
    return new LongFieldReaderColumn(frame, signature.indexOf(columnName), signature.size());
  }

  private class LongFieldReaderColumn implements Column
  {
    private final Frame frame;
    private final Memory dataRegion;
    private final FieldPositionHelper coach;

    public LongFieldReaderColumn(Frame frame, int columnIndex, int numFields)
    {
      this.frame = frame;
      this.dataRegion = frame.region(RowBasedFrameWriter.ROW_DATA_REGION);

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
          return ColumnType.LONG;
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
            return getLongAtPosition(fieldPosition);
          }
        }

        @Override
        public double getDouble(int rowNum)
        {
          return getLong(rowNum);
        }

        @Override
        public float getFloat(int rowNum)
        {
          return getLong(rowNum);
        }

        @Override
        public long getLong(int rowNum)
        {
          final long fieldPosition = coach.computeFieldPosition(rowNum);

          if (dataRegion.getByte(fieldPosition) == getNullIndicatorByte()) {
            return 0L;
          } else {
            return getLongAtPosition(fieldPosition);
          }
        }

        @Override
        public int getInt(int rowNum)
        {
          return (int) getLong(rowNum);
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
              return Long.compare(getLongAtPosition(lhsPosition), getLongAtPosition(rhsPosition));
            }
          }
        }

        private long getLongAtPosition(long rhsPosition)
        {
          return TransformUtils.detransformToLong(dataRegion.getLong(rhsPosition + Byte.BYTES));
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
