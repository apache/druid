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

package io.druid.segment;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.java.util.common.IAE;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;

import java.util.BitSet;
import java.util.Objects;

public final class DimensionSelectorUtils
{

  private DimensionSelectorUtils()
  {
  }

  /**
   * Generic implementation of {@link DimensionSelector#makeValueMatcher(String)}, uses {@link
   * DimensionSelector#getRow()} of the given {@link DimensionSelector}. "Lazy" DimensionSelectors could delegate
   * {@code makeValueMatcher()} to this method, but encouraged to implement {@code makeValueMatcher()} themselves,
   * bypassing the {@link IndexedInts} abstraction.
   */
  public static ValueMatcher makeValueMatcherGeneric(DimensionSelector selector, String value)
  {
    IdLookup idLookup = selector.idLookup();
    if (idLookup != null) {
      return makeDictionaryEncodedValueMatcherGeneric(selector, idLookup.lookupId(value), value == null);
    } else if (selector.getValueCardinality() >= 0 && selector.nameLookupPossibleInAdvance()) {
      // Employ precomputed BitSet optimization
      return makeDictionaryEncodedValueMatcherGeneric(selector, Predicates.equalTo(value));
    } else {
      return makeNonDictionaryEncodedValueMatcherGeneric(selector, value);
    }
  }

  private static ValueMatcher makeDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      final int valueId,
      final boolean matchNull
  )
  {
    if (valueId >= 0) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              if (row.get(i) == valueId) {
                return true;
              }
            }
            return false;
          }
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
        }
      };
    } else {
      if (matchNull) {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            return size == 0;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      } else {
        return BooleanValueMatcher.of(false);
      }
    }
  }

  private static ValueMatcher makeNonDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      final String value
  )
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return value == null;
        } else {
          for (int i = 0; i < size; ++i) {
            if (Objects.equals(selector.lookupName(row.get(i)), value)) {
              return true;
            }
          }
          return false;
        }
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  /**
   * Generic implementation of {@link DimensionSelector#makeValueMatcher(Predicate)}, uses {@link
   * DimensionSelector#getRow()} of the given {@link DimensionSelector}. "Lazy" DimensionSelectors could delegate
   * {@code makeValueMatcher()} to this method, but encouraged to implement {@code makeValueMatcher()} themselves,
   * bypassing the {@link IndexedInts} abstraction.
   */
  public static ValueMatcher makeValueMatcherGeneric(DimensionSelector selector, Predicate<String> predicate)
  {
    int cardinality = selector.getValueCardinality();
    if (cardinality >= 0 && selector.nameLookupPossibleInAdvance()) {
      return makeDictionaryEncodedValueMatcherGeneric(selector, predicate);
    } else {
      return makeNonDictionaryEncodedValueMatcherGeneric(selector, predicate);
    }
  }

  private static ValueMatcher makeDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      Predicate<String> predicate
  )
  {
    final BitSet predicateMatchingValueIds = makePredicateMatchingSet(selector, predicate);
    final boolean matchNull = predicate.apply(null);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return matchNull;
        } else {
          for (int i = 0; i < size; ++i) {
            if (predicateMatchingValueIds.get(row.get(i))) {
              return true;
            }
          }
          return false;
        }
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  private static ValueMatcher makeNonDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      final Predicate<String> predicate
  )
  {
    final boolean matchNull = predicate.apply(null);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return matchNull;
        } else {
          for (int i = 0; i < size; ++i) {
            if (predicate.apply(selector.lookupName(row.get(i)))) {
              return true;
            }
          }
          return false;
        }
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  public static BitSet makePredicateMatchingSet(DimensionSelector selector, Predicate<String> predicate)
  {
    if (!selector.nameLookupPossibleInAdvance()) {
      throw new IAE("selector.nameLookupPossibleInAdvance() should return true");
    }
    int cardinality = selector.getValueCardinality();
    BitSet valueIds = new BitSet(cardinality);
    for (int i = 0; i < cardinality; i++) {
      if (predicate.apply(selector.lookupName(i))) {
        valueIds.set(i);
      }
    }
    return valueIds;
  }
}
