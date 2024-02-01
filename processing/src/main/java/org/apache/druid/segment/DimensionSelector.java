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

package org.apache.druid.segment;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Selector for a string-typed column, either single- or multi-valued. This is named a "dimension" selector for legacy
 * reasons: in the past, all Druid dimensions were string-typed.
 *
 * @see org.apache.druid.segment.vector.SingleValueDimensionVectorSelector, a vectorized version
 * @see org.apache.druid.segment.vector.MultiValueDimensionVectorSelector, another vectorized version
 */
@PublicApi
public interface DimensionSelector extends ColumnValueSelector<Object>, DimensionDictionarySelector, HotLoopCallee
{
  /**
   * Returns the indexed values at the current position in this DimensionSelector.
   *
   * IMPORTANT. The returned {@link IndexedInts} object could generally be reused inside the implementation of
   * DimensionSelector, i. e. this method could always return the same object for the same selector. Users
   * of this API, such as {@link org.apache.druid.query.aggregation.Aggregator#aggregate()}, {@link
   * org.apache.druid.query.aggregation.BufferAggregator#aggregate}, {@link org.apache.druid.query.aggregation.AggregateCombiner#reset},
   * {@link org.apache.druid.query.aggregation.AggregateCombiner#fold} should be prepared for that and not storing the object
   * returned from this method in their state, assuming that the object will remain unchanged even when the position of
   * the selector changes. This may not be the case.
   */
  @CalledFromHotLoop
  IndexedInts getRow();

  /**
   * @param value nullable dimension value
   */
  ValueMatcher makeValueMatcher(@Nullable String value);

  ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory);

  /**
   * @deprecated This method is marked as deprecated in DimensionSelector to minimize the probability of accidental
   * calling. "Polymorphism" of DimensionSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default float getFloat()
  {
    // This is controversial, see https://github.com/apache/druid/issues/4888
    return 0.0f;
  }

  /**
   * @deprecated This method is marked as deprecated in DimensionSelector to minimize the probability of accidental
   * calling. "Polymorphism" of DimensionSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default double getDouble()
  {
    // This is controversial, see https://github.com/apache/druid/issues/4888
    return 0.0;
  }

  /**
   * @deprecated This method is marked as deprecated in DimensionSelector to minimize the probability of accidental
   * calling. "Polymorphism" of DimensionSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default long getLong()
  {
    // This is controversial, see https://github.com/apache/druid/issues/4888
    return 0L;
  }

  @Deprecated
  @Override
  default boolean isNull()
  {
    return false;
  }

  /**
   * Converts the current result of {@link #getRow()} into null, if the row is empty, a String, if the row has size 1,
   * or a {@code List<String>}, if the row has size > 1, using {@link #lookupName(int)}.
   *
   * This method is not the default implementation of {@link #getObject()} to minimize the chance that implementations
   * "forget" to override it with more optimized version.
   */
  @Nullable
  default Object defaultGetObject()
  {
    return rowToObject(getRow(), this);
  }

  /**
   * Converts a particular {@link IndexedInts} to an Object in a standard way, assuming each element in the IndexedInts
   * is a dictionary ID that can be resolved with the provided selector.
   *
   * Specification:
   * 1) Empty row ({@link IndexedInts#size()} zero) returns null.
   * 2) Single-value row returns a single {@link String}.
   * 3) Two+ value rows return {@link List} of {@link String}.
   */
  @Nullable
  static Object rowToObject(IndexedInts row, DimensionDictionarySelector selector)
  {
    int rowSize = row.size();
    if (rowSize == 0) {
      return null;
    } else if (rowSize == 1) {
      return selector.lookupName(row.get(0));
    } else {
      final String[] strings = new String[rowSize];
      for (int i = 0; i < rowSize; i++) {
        strings[i] = selector.lookupName(row.get(i));
      }
      return Arrays.asList(strings);
    }
  }

  static DimensionSelector nilSelector()
  {
    return NullDimensionSelectorHolder.NULL_DIMENSION_SELECTOR;
  }

  static DimensionSelector constant(@Nullable final String value)
  {
    if (NullHandling.isNullOrEquivalent(value)) {
      return nilSelector();
    } else {
      return new ConstantDimensionSelector(value);
    }
  }

  static DimensionSelector constant(@Nullable final String value, @Nullable final ExtractionFn extractionFn)
  {
    if (extractionFn == null) {
      return constant(value);
    } else {
      return constant(extractionFn.apply(value));
    }
  }

  static DimensionSelector multiConstant(@Nullable final List<String> values)
  {
    // this method treats null, [], and [null] equivalently as null
    if (values == null || values.isEmpty()) {
      return NullDimensionSelectorHolder.NULL_DIMENSION_SELECTOR;
    } else if (values.size() == 1) {
      // the single value constant selector is more optimized than the multi-value constant selector because the latter
      // does not report value cardinality, but otherwise behaves identically when used for grouping or selecting to a
      // normal multi-value dimension selector (getObject on a row with a single value returns the object instead of
      // the list)
      return constant(values.get(0));
    } else {
      return new ConstantMultiValueDimensionSelector(values);
    }
  }

  static DimensionSelector multiConstant(@Nullable final List<String> values, @Nullable final ExtractionFn extractionFn)
  {
    if (extractionFn == null) {
      return multiConstant(values);
    } else {
      if (values == null) {
        // the single value constant selector is more optimized than the multi-value constant selector because the
        // latter does not report value cardinality, but otherwise behaves identically when used for grouping or
        // selecting to a normal multi-value dimension selector (getObject on a row with a single value returns the
        // object instead of the list)
        return constant(extractionFn.apply(null));
      }
      return multiConstant(values.stream().map(extractionFn::apply).collect(Collectors.toList()));
    }
  }

  /**
   * Checks if the given selector constantly returns null. This method could be used in the beginning of execution of
   * some queries and making some aggregations for heuristic shortcuts.
   */
  static boolean isNilSelector(final DimensionSelector selector)
  {
    return selector.nameLookupPossibleInAdvance()
           && selector.getValueCardinality() == 1
           && selector.lookupName(0) == null;
  }

  /**
   * This class not a public API. It is needed solely to make NULL_DIMENSION_SELECTOR and NullDimensionSelector private
   * and inaccessible even from classes in the same package. It could be removed, when Druid is updated to at least Java
   * 9, that supports private members in interfaces.
   */
  class NullDimensionSelectorHolder
  {
    private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

    /**
     * This class is specially made a nested member of {@link DimensionSelector} interface, and accessible only through
     * calling DimensionSelector.constant(null), so that it's impossible to mistakely use NullDimensionSelector in
     * instanceof statements. {@link #isNilSelector} method should be used instead.
     */
    private static class NullDimensionSelector implements SingleValueHistoricalDimensionSelector, IdLookup
    {
      private NullDimensionSelector()
      {
        // Singleton.
      }

      @Override
      public IndexedInts getRow()
      {
        return ZeroIndexedInts.instance();
      }

      @Override
      public int getRowValue(int offset)
      {
        return 0;
      }

      @Override
      public IndexedInts getRow(int offset)
      {
        return getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        if (NullHandling.isNullOrEquivalent(value)) {
          return ValueMatchers.allTrue();
        }
        return ValueMatchers.allUnknown();
      }

      @Override
      public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
      {
        final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
        final DruidPredicateMatch match = predicate.apply(null);

        if (match == DruidPredicateMatch.TRUE) {
          return ValueMatchers.allTrue();
        }
        if (match == DruidPredicateMatch.UNKNOWN) {
          return ValueMatchers.allUnknown();
        }
        return ValueMatchers.allFalse();
      }

      @Override
      public int getValueCardinality()
      {
        return 1;
      }

      @Override
      @Nullable
      public String lookupName(int id)
      {
        assert id == 0 : "id = " + id;
        return null;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return this;
      }

      @Override
      public int lookupId(@Nullable String name)
      {
        return NullHandling.isNullOrEquivalent(name) ? 0 : -1;
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return null;
      }

      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }

      @Override
      public boolean isNull()
      {
        return NullHandling.sqlCompatible();
      }
    }
  }
}
