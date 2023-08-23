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
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class NumericFieldReader<T extends Number> implements FieldReader
{

  private final byte nullIndicatorByte;

  public NumericFieldReader(boolean forArray)
  {
    if (!forArray) {
      this.nullIndicatorByte = NumericFieldWriter.NULL_BYTE;
    } else {
      this.nullIndicatorByte = NumericFieldWriter.ARRAY_NULL_BYTE;
    }
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

  public abstract ValueType getValueType();

  public abstract Class<? extends T> getClassOfObject();

  public abstract T getValueFromMemory(Memory memory, long position);

  public class Selector implements ColumnValueSelector<T>
  {

    private final Memory dataRegion;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory dataRegion, final ReadableFieldPointer fieldPointer)
    {
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public double getDouble()
    {
      return getObject().doubleValue();
    }

    @Override
    public float getFloat()
    {
      return getObject().floatValue();
    }

    @Override
    public long getLong()
    {
      return getObject().longValue();
    }

    @Override
    public boolean isNull()
    {
      return NumericFieldReader.this.isNull(dataRegion, fieldPointer.position());
    }

    @Nonnull
    @Override
    public T getObject()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return NumericFieldReader.this.getValueFromMemory(dataRegion, fieldPointer.position() + Byte.BYTES);
    }

    @Override
    public Class<? extends T> classOfObject()
    {
      return getClassOfObject();
    }
  }
}
