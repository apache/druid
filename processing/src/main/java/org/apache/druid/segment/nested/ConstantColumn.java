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

package org.apache.druid.segment.nested;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.BaseFloatVectorValueSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public class ConstantColumn implements NestedCommonFormatColumn, DictionaryEncodedColumn<String>
{
  private final ColumnType logicalType;
  private final ExprEval<?> value;
  private final String stringVal;

  public ConstantColumn(ColumnType logicalType, @Nullable Object value)
  {
    this.logicalType = logicalType;
    this.value = ExprEval.ofType(ExpressionType.fromColumnType(logicalType), value);
    this.stringVal = value == null ? null : String.valueOf(value);
  }

  @Override
  public int length()
  {
    return 0;
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return 0;
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException("Not a multi-value column");
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    if (id == 0) {
      return stringVal;
    }
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    if (Objects.equals(name, stringVal)) {
      return 0;
    }
    return -1;
  }

  @Override
  public int getCardinality()
  {
    return 1;
  }

  @Override
  public DimensionSelector makeDimensionSelector(ReadableOffset offset, @Nullable ExtractionFn extractionFn)
  {
    return DimensionSelector.constant(stringVal, extractionFn);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ColumnValueSelector<Object>()
    {
      @Override
      public double getDouble()
      {
        return value.asDouble();
      }

      @Override
      public float getFloat()
      {
        return (float) value.asDouble();
      }

      @Override
      public long getLong()
      {
        return value.asLong();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        return value.isNumericNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return value.valueOrDefault();
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }
    };
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    return ConstantVectorSelectors.singleValueDimensionVectorSelector(vectorOffset, stringVal);
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException("Not a multi-value column");
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    if (logicalType.isNumeric()) {
      final boolean[] nulls;
      if (value.isNumericNull()) {
        nulls = new boolean[offset.getMaxVectorSize()];
        Arrays.fill(nulls, true);
      } else {
        nulls = null;
      }
      switch (logicalType.getType()) {
        case LONG:
          final long[] constantLong = new long[offset.getMaxVectorSize()];
          Arrays.fill(constantLong, value.asLong());
          return new BaseLongVectorValueSelector(offset)
          {
            @Override
            public long[] getLongVector()
            {
              return constantLong;
            }

            @Nullable
            @Override
            public boolean[] getNullVector()
            {
              return nulls;
            }
          };

        case DOUBLE:
          final double[] constantDouble = new double[offset.getMaxVectorSize()];
          Arrays.fill(constantDouble, value.asDouble());
          return new BaseDoubleVectorValueSelector(offset)
          {
            @Override
            public double[] getDoubleVector()
            {
              return constantDouble;
            }

            @Nullable
            @Override
            public boolean[] getNullVector()
            {
              return nulls;
            }
          };

        case FLOAT:
          final float[] constantFloat = new float[offset.getMaxVectorSize()];
          Arrays.fill(constantFloat, (float) value.asDouble());
          return new BaseFloatVectorValueSelector(offset)
          {
            @Override
            public float[] getFloatVector()
            {
              return constantFloat;
            }

            @Nullable
            @Override
            public boolean[] getNullVector()
            {
              return nulls;
            }
          };
      }
    }
    throw new UOE("Cannot make VectorValueSelector for constant column of type[%s]", logicalType);
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    final Object[] constant = new Object[offset.getMaxVectorSize()];
    Arrays.fill(constant, value.valueOrDefault());
    return new VectorObjectSelector()
    {

      @Override
      public Object[] getObjectVector()
      {
        return constant;
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }
    };
  }

  @Override
  public Indexed<String> getStringDictionary()
  {
    if (logicalType.is(ValueType.STRING)) {
      return new ListIndexed<>(Collections.singletonList(value.asString()));
    }
    if (logicalType.equals(ColumnType.STRING_ARRAY)) {
      Object[] array = value.asArray();
      if (array == null) {
        return new ListIndexed<>(Collections.singletonList(null));
      }
      String[] strings = Arrays.copyOf(array, array.length, String[].class);
      Arrays.sort(strings, ColumnType.STRING.getNullableStrategy());
      return new ListIndexed<>(Arrays.asList(strings));
    }
    return Indexed.empty();
  }

  @Override
  public Indexed<Long> getLongDictionary()
  {
    if (logicalType.is(ValueType.LONG)) {
      return new ListIndexed<>(Collections.singletonList((Long) value.valueOrDefault()));
    }
    if (logicalType.equals(ColumnType.LONG_ARRAY)) {
      Object[] array = value.asArray();
      if (array == null) {
        return new ListIndexed<>(Collections.singletonList(null));
      }
      Long[] longs = Arrays.copyOf(array, array.length, Long[].class);
      Arrays.sort(longs, ColumnType.LONG.getNullableStrategy());
      return new ListIndexed<>(Arrays.asList(longs));
    }
    return Indexed.empty();
  }

  @Override
  public Indexed<Double> getDoubleDictionary()
  {
    if (logicalType.is(ValueType.DOUBLE)) {
      return new ListIndexed<>(Collections.singletonList((Double) value.valueOrDefault()));
    }
    if (logicalType.equals(ColumnType.DOUBLE_ARRAY)) {
      Object[] array = value.asArray();
      if (array == null) {
        return new ListIndexed<>(Collections.singletonList(null));
      }
      Double[] doubles = Arrays.copyOf(array, array.length, Double[].class);
      Arrays.sort(doubles, ColumnType.DOUBLE.getNullableStrategy());
      return new ListIndexed<>(Arrays.asList(doubles));
    }
    return Indexed.empty();
  }

  @Override
  public Indexed<Object[]> getArrayDictionary()
  {
    if (logicalType.isArray()) {
      return new ListIndexed<>(Collections.singletonList(value.asArray()));
    }
    return Indexed.empty();
  }

  @Override
  public ColumnType getLogicalType()
  {
    return logicalType;
  }

  @Nullable
  public Object getConstantValue()
  {
    return value.valueOrDefault();
  }

  @Override
  public void close() throws IOException
  {
    // nothing to close
  }
}
