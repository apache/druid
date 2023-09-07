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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class NumericArrayFieldReader implements FieldReader
{
  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    throw DruidException.defensive("Cannot call makeDimensionSelector on field of type ARRAY");
  }

  @Override
  public boolean isNull(Memory memory, long position)
  {
    final byte firstByte = memory.getByte(position);
    return firstByte == NumericArrayFieldWriter.NULL_ROW;
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  public abstract static class Selector<T extends Number> implements ColumnValueSelector
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;

    private long currentFieldPosition = -1;

    private final List<T> currentRow = new ArrayList<>();
    private boolean currentRowIsNull;

    public Selector(final Memory memory, final ReadableFieldPointer fieldPointer)
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Nullable
    @Override
    public Object getObject()
    {
      final List<T> currentArray = computeCurrentArray();

      if (currentArray == null) {
        return null;
      }

      return currentArray.toArray();
    }

    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public double getDouble()
    {
      return 0;
    }

    @Override
    public float getFloat()
    {
      return 0;
    }

    @Override
    public long getLong()
    {
      return 0;
    }

    @Override
    public boolean isNull()
    {
      long position = fieldPointer.position();
      final byte firstByte = memory.getByte(position);
      return firstByte == NumericArrayFieldWriter.NULL_ROW;
    }

    @Nullable
    public abstract T getIndividualValueAtMemory(Memory memory, long position);

    public abstract int getIndividualFieldSize();

    @Nullable
    private List<T> computeCurrentArray()
    {
      final long fieldPosition = fieldPointer.position();

      if (fieldPosition != currentFieldPosition) {
        updateCurrentArray(fieldPosition);
      }

      this.currentFieldPosition = fieldPosition;

      if (currentRowIsNull) {
        return null;
      }
      return currentRow;

    }

    private void updateCurrentArray(final long fieldPosition)
    {
      currentRow.clear();
      currentRowIsNull = false;

      long position = fieldPosition;
      long limit = memory.getCapacity();

      if (isNull()) {
        currentRowIsNull = true;
        return;
      }

      // Adding a check here to prevent the position from potentially overflowing
      if (position < limit) {
        position++;
      }

      boolean rowTerminatorSeen = false;

      while (position < limit) {
        final byte kind = memory.getByte(position);

        if (kind == NumericArrayFieldWriter.ARRAY_TERMINATOR) {
          rowTerminatorSeen = true;
          break;
        }

        currentRow.add(getIndividualValueAtMemory(memory, position));
        position += getIndividualFieldSize();
      }

      if (!rowTerminatorSeen) {
        throw DruidException.defensive("Unexpected end of field");
      }
    }
  }
}
