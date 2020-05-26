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

package org.apache.druid.segment.filter;

import com.google.common.base.Preconditions;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ColumnComparisonFilter implements Filter
{
  private final List<DimensionSpec> dimensions;

  public ColumnComparisonFilter(final List<DimensionSpec> dimensions)
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions");
  }

  @Override
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final List<Supplier<String[]>> valueGetters = new ArrayList<>(dimensions.size());

    for (final DimensionSpec dimension : dimensions) {
      valueGetters.add(ColumnProcessors.makeProcessor(dimension, ColumnComparisonReaderFactory.INSTANCE, factory));
    }

    return makeValueMatcher(valueGetters);
  }

  public static ValueMatcher makeValueMatcher(final List<Supplier<String[]>> valueGetters)
  {
    if (valueGetters.isEmpty()) {
      return BooleanValueMatcher.of(true);
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        // Keep all values to compare against each other.
        String[][] values = new String[valueGetters.size()][];

        for (int i = 0; i < valueGetters.size(); i++) {
          values[i] = valueGetters.get(i).get();
          // Compare the new values to the values we already got.
          for (int j = 0; j < i; j++) {
            if (!overlap(values[i], values[j])) {
              return false;
            }
          }
        }
        return true;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // All value getters are likely the same or similar (in terms of runtime shape), so inspecting only one of them.
        inspector.visit("oneValueGetter", valueGetters.get(0));
      }
    };
  }

  /**
   * overlap returns true when: as and bs have one or more elements in common,
   * as and bs are both null, or as and bs are both empty.
   */
  public static boolean overlap(@Nullable String[] as, @Nullable String[] bs)
  {
    if (as == null || bs == null) {
      // They only have overlap if both are null.
      return as == null && bs == null;
    }
    if (as.length == 0 && bs.length == 0) {
      return true;
    }

    for (String a : as) {
      for (String b : bs) {
        if (Objects.equals(a, b)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return false;
  }

  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return false;
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    return false;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return dimensions.stream().map(DimensionSpec::getDimension).collect(Collectors.toSet());
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    throw new UnsupportedOperationException();
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnComparisonFilter that = (ColumnComparisonFilter) o;
    return Objects.equals(dimensions, that.dimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions);
  }

  private static class ColumnComparisonReaderFactory implements ColumnProcessorFactory<Supplier<String[]>>
  {
    private static final ColumnComparisonReaderFactory INSTANCE = new ColumnComparisonReaderFactory();
    private static final String[] NULL_VALUE = new String[]{null};

    @Override
    public ValueType defaultType()
    {
      return ValueType.STRING;
    }

    @Override
    public Supplier<String[]> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      return () -> {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          return NULL_VALUE;
        } else {
          String[] values = new String[size];
          for (int i = 0; i < size; ++i) {
            values[i] = selector.lookupName(row.get(i));
          }
          return values;
        }
      };
    }

    @Override
    public Supplier<String[]> makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      return () -> {
        if (selector.isNull()) {
          return NULL_VALUE;
        }
        return new String[]{Float.toString(selector.getFloat())};
      };
    }

    @Override
    public Supplier<String[]> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      return () -> {
        if (selector.isNull()) {
          return NULL_VALUE;
        }
        return new String[]{Double.toString(selector.getDouble())};
      };
    }

    @Override
    public Supplier<String[]> makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      return () -> {
        if (selector.isNull()) {
          return NULL_VALUE;
        }
        return new String[]{Long.toString(selector.getLong())};
      };
    }

    @Override
    public Supplier<String[]> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return () -> NULL_VALUE;
    }
  }
}
