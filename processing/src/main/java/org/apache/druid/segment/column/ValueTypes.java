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

public class ValueTypes
{
  public static SettableColumnValueSelector<?> makeNewSettableColumnValueSelector(ValueType valueType)
  {
    switch (valueType) {
      case DOUBLE:
        return new SettableDoubleColumnValueSelector();
      case FLOAT:
        return new SettableFloatColumnValueSelector();
      case LONG:
        return new SettableLongColumnValueSelector();
      case STRING:
        return new SettableDimensionValueSelector();
      default:
        return new SettableObjectColumnValueSelector<>();
    }
  }

  public static DimensionSelector makeNumericWrappingDimensionSelector(
      ValueType valueType,
      ColumnValueSelector<?> numericColumnValueSelector,
      @Nullable ExtractionFn extractionFn
  )
  {
    switch (valueType) {
      case DOUBLE:
        return new DoubleWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
      case FLOAT:
        return new FloatWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
      case LONG:
        return new LongWrappingDimensionSelector(numericColumnValueSelector, extractionFn);
      default:
        throw new UnsupportedOperationException("Not a numeric value type: " + valueType.name());
    }
  }

  private ValueTypes()
  {
    // no instantiation
  }
}
