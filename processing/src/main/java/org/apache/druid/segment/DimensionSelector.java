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

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 */
@PublicApi
public interface DimensionSelector extends ColumnValueSelector<Object>, HotLoopCallee
{
  int CARDINALITY_UNKNOWN = -1;

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

  ValueMatcher makeValueMatcher(Predicate<String> predicate);

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * Cardinality may be unknown (e.g. the selector used by IncrementalIndex while reading input rows),
   * in which case this method will return -1. If cardinality is unknown, you should assume this
   * dimension selector has no dictionary, and avoid storing ids, calling "lookupId", or calling "lookupName"
   * outside of the context of operating on a single row.
   *
   * @return the value cardinality, or -1 if unknown.
   */
  int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) =&gt; [0 1]
   * getRow(1) =&gt; [0]
   * getRow(2) =&gt; [0 1]
   * getRow(3) =&gt; [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) =&gt; A
   * lookupName(1) =&gt; B
   *
   * @param id id to lookup the field name for
   * @return the field name for the given id
   */
  @CalledFromHotLoop
  @Nullable
  String lookupName(int id);

  /**
   * Returns true if it is possible to {@link #lookupName(int)} by ids from 0 to {@link #getValueCardinality()}
   * before the rows with those ids are returned.
   *
   * <p>Returns false if {@link #lookupName(int)} could be called with ids, returned from the most recent call of {@link
   * #getRow()} on this DimensionSelector, but not earlier. If {@link #getValueCardinality()} of this DimensionSelector
   * additionally returns {@link #CARDINALITY_UNKNOWN}, {@code lookupName()} couldn't be called with ids, returned by
   * not the most recent call of {@link #getRow()}, i. e. names for ids couldn't be looked up "later". If {@link
   * #getValueCardinality()} returns a non-negative number, {@code lookupName()} could be called with any ids, returned
   * from {@code #getRow()} since the creation of this DimensionSelector.
   *
   * <p>If {@link #lookupName(int)} is called with an ineligible id, result is undefined: exception could be thrown, or
   * null returned, or some other random value.
   */
  boolean nameLookupPossibleInAdvance();

  /**
   * Returns {@link IdLookup} if available for this DimensionSelector, or null.
   */
  @Nullable
  IdLookup idLookup();

  /**
   * @deprecated This method is marked as deprecated in DimensionSelector to minimize the probability of accidental
   * calling. "Polymorphism" of DimensionSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default float getFloat()
  {
    // This is controversial, see https://github.com/apache/incubator-druid/issues/4888
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
    // This is controversial, see https://github.com/apache/incubator-druid/issues/4888
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
    // This is controversial, see https://github.com/apache/incubator-druid/issues/4888
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
   * or a String[] array, if the row has size > 1, using {@link #lookupName(int)}.
   *
   * This method is not the default implementation of {@link #getObject()} to minimize the chance that implementations
   * "forget" to override it with more optimized version.
   */
  @Nullable
  default Object defaultGetObject()
  {
    IndexedInts row = getRow();
    int rowSize = row.size();
    if (rowSize == 0) {
      return null;
    } else if (rowSize == 1) {
      return lookupName(row.get(0));
    } else {
      final String[] strings = new String[rowSize];
      for (int i = 0; i < rowSize; i++) {
        strings[i] = lookupName(row.get(i));
      }
      return Arrays.asList(strings);
    }
  }

  static DimensionSelector constant(@Nullable final String value)
  {
    if (NullHandling.isNullOrEquivalent(value)) {
      return NullDimensionSelectorHolder.NULL_DIMENSION_SELECTOR;
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
        return BooleanValueMatcher.of(value == null);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return BooleanValueMatcher.of(predicate.apply(null));
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
    }
  }
}
