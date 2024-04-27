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
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

/**
 * Reads the fields created by the {@link NumericFieldWriter}. See the Javadoc for the writer for format details
 *
 * @see NumericFieldWriter
 */
public abstract class NumericFieldReader implements FieldReader
{

  /**
   * The indicator byte which denotes that the following value is null.
   */
  private final byte nullIndicatorByte;

  public NumericFieldReader(boolean forArray)
  {
    if (!forArray) {
      this.nullIndicatorByte = NumericFieldWriter.NULL_BYTE;
    } else {
      this.nullIndicatorByte = NumericFieldWriter.ARRAY_ELEMENT_NULL_BYTE;
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return getColumnValueSelector(memory, fieldPointer, nullIndicatorByte);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    return ValueTypes.makeNumericWrappingDimensionSelector(
        getValueType(),
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isNull(Memory memory, long position)
  {
    return memory.getByte(position) == nullIndicatorByte;
  }


  @Override
  public boolean isComparable()
  {
    return true;
  }

  /**
   * Creates a column value selector for the element written at fieldPointer's position in the memory.
   * The nullilty check is handled by the nullIndicatorByte
   */
  public abstract ColumnValueSelector<?> getColumnValueSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      byte nullIndicatorByte
  );

  /**
   * {@link ValueType} of the inheritor's element
   */
  public abstract ValueType getValueType();

  /**
   * Helper class which allows the inheritors to fetch the nullity of the field located at fieldPointer's position in
   * the dataRegion.
   *
   * The implementations of the column value selectors returned by the {@link #getColumnValueSelector} can inherit this
   * class and call {@link #_isNull()} in their {@link ColumnValueSelector#isNull()} to offload the responsibility of
   * detecting null elements to this Selector, instead of reworking the null handling
   */
  public abstract static class Selector
  {
    private final Memory dataRegion;
    private final ReadableFieldPointer fieldPointer;
    private final byte nullIndicatorByte;

    public Selector(
        final Memory dataRegion,
        final ReadableFieldPointer fieldPointer,
        final byte nullIndicatorByte
    )
    {
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
      this.nullIndicatorByte = nullIndicatorByte;
    }

    protected boolean _isNull()
    {
      return dataRegion.getByte(fieldPointer.position()) == nullIndicatorByte;
    }
  }
}
