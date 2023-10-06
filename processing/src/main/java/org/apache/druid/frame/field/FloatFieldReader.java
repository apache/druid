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
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.column.ValueType;

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
}
