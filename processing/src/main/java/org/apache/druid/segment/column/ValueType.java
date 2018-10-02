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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleWrappingDimensionSelector;
import org.apache.druid.segment.FloatWrappingDimensionSelector;
import org.apache.druid.segment.LongWrappingDimensionSelector;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableDimensionValueSelector;
import org.apache.druid.segment.selector.settable.SettableDoubleColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableFloatColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableObjectColumnValueSelector;

import javax.annotation.Nullable;

/**
 * Should be the same as {@link org.apache.druid.data.input.impl.DimensionSchema.ValueType}.
 * TODO merge them when druid-api is merged back into the main repo
 */
public enum ValueType
{
  FLOAT {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        @Nullable ExtractionFn extractionFn
    )
    {
      return new FloatWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return new SettableFloatColumnValueSelector();
    }
  },
  DOUBLE {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        @Nullable ExtractionFn extractionFn
    )
    {
      return new DoubleWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return new SettableDoubleColumnValueSelector();
    }
  },
  LONG {
    @Override
    public DimensionSelector makeNumericWrappingDimensionSelector(
        ColumnValueSelector numericColumnValueSelector,
        @Nullable ExtractionFn extractionFn
    )
    {
      return new LongWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return new SettableLongColumnValueSelector();
    }
  },
  STRING {
    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return new SettableDimensionValueSelector();
    }
  },
  COMPLEX {
    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return new SettableObjectColumnValueSelector();
    }
  };

  public DimensionSelector makeNumericWrappingDimensionSelector(
      ColumnValueSelector numericColumnValueSelector,
      @Nullable ExtractionFn extractionFn
  )
  {
    throw new UnsupportedOperationException("Not a numeric value type: " + name());
  }

  public abstract SettableColumnValueSelector makeNewSettableColumnValueSelector();

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
