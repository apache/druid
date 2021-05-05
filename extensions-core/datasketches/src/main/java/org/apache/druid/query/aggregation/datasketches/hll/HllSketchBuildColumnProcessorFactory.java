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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Scalar (non-vectorized) column processor factory.
 */
public class HllSketchBuildColumnProcessorFactory implements ColumnProcessorFactory<Consumer<Supplier<HllSketch>>>
{
  private final StringEncoding stringEncoding;

  HllSketchBuildColumnProcessorFactory(StringEncoding stringEncoding)
  {
    this.stringEncoding = stringEncoding;
  }

  @Override
  public ValueType defaultType()
  {
    return ValueType.STRING;
  }

  @Override
  public Consumer<Supplier<HllSketch>> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
  {
    return sketch -> {
      final IndexedInts row = selector.getRow();
      final int sz = row.size();

      for (int i = 0; i < sz; i++) {
        HllSketchBuildUtil.updateSketchWithDictionarySelector(sketch.get(), stringEncoding, selector, row.get(i));
      }
    };
  }

  @Override
  public Consumer<Supplier<HllSketch>> makeFloatProcessor(BaseFloatColumnValueSelector selector)
  {
    return sketch -> {
      if (!selector.isNull()) {
        // Important that this is *double* typed, since HllSketchBuildAggregator treats doubles and floats the same.
        final double value = selector.getFloat();
        sketch.get().update(value);
      }
    };
  }

  @Override
  public Consumer<Supplier<HllSketch>> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
  {
    return sketch -> {
      if (!selector.isNull()) {
        sketch.get().update(selector.getDouble());
      }
    };
  }

  @Override
  public Consumer<Supplier<HllSketch>> makeLongProcessor(BaseLongColumnValueSelector selector)
  {
    return sketch -> {
      if (!selector.isNull()) {
        sketch.get().update(selector.getLong());
      }
    };
  }

  @Override
  public Consumer<Supplier<HllSketch>> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
  {
    return sketch -> {
      final Object o = selector.getObject();

      if (o != null) {
        HllSketchBuildUtil.updateSketch(sketch.get(), stringEncoding, o);
      }
    };
  }
}
