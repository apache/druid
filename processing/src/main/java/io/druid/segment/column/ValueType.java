/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleWrappingDimensionSelector;
import io.druid.segment.FloatWrappingDimensionSelector;
import io.druid.segment.LongWrappingDimensionSelector;
import io.druid.segment.selector.settable.SettableColumnValueSelector;
import io.druid.segment.selector.settable.SettableDimensionValueSelector;
import io.druid.segment.selector.settable.SettableDoubleColumnValueSelector;
import io.druid.segment.selector.settable.SettableFloatColumnValueSelector;
import io.druid.segment.selector.settable.SettableLongColumnValueSelector;
import io.druid.segment.selector.settable.SettableObjectColumnValueSelector;

/**
 * Should be the same as {@link io.druid.data.input.impl.DimensionSchema.ValueType}.
 * TODO merge them when druid-api is merged back into the main repo
 */
public enum ValueType
{
  FLOAT {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        ExtractionFn extractionFn
    )
    {
      return new FloatWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeSettableColumnValueSelector()
    {
      return new SettableFloatColumnValueSelector();
    }
  },
  DOUBLE {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        ExtractionFn extractionFn
    )
    {
      return new DoubleWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeSettableColumnValueSelector()
    {
      return new SettableDoubleColumnValueSelector();
    }
  },
  LONG {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        ExtractionFn extractionFn
    )
    {
      return new LongWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeSettableColumnValueSelector()
    {
      return new SettableLongColumnValueSelector();
    }
  },
  STRING {
    @Override
    public SettableColumnValueSelector makeSettableColumnValueSelector()
    {
      return new SettableDimensionValueSelector();
    }
  },
  COMPLEX {
    @Override
    public SettableColumnValueSelector makeSettableColumnValueSelector()
    {
      return new SettableObjectColumnValueSelector();
    }
  };

  public DimensionSelector makeNumericWrappingDimensionSelector(
      ColumnValueSelector numericColumnValueSelector,
      ExtractionFn extractionFn
  )
  {
    throw new UnsupportedOperationException("Not a numeric value type: " + name());
  }

  public abstract SettableColumnValueSelector makeSettableColumnValueSelector();

  public boolean isNumeric()
  {
    return isNumeric(this);
  }

  @JsonCreator
  public static ValueType fromString(String name)
  {
    if (name == null) {
      return null;
    }
    return valueOf(StringUtils.toUpperCase(name));
  }

  public static boolean isNumeric(ValueType type)
  {
    if (type == ValueType.LONG || type == ValueType.FLOAT || type == ValueType.DOUBLE) {
      return true;
    }
    return false;
  }
}
