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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
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
  public static ValueMatcher makeValueMatcherGeneric(DimensionSelector selector, @Nullable String value)
  {
    IdLookup idLookup = selector.idLookup();
    if (idLookup != null) {
      return makeDictionaryEncodedValueMatcherGeneric(selector, idLookup.lookupId(value), value == null);
    } else if (selector.getValueCardinality() >= 0 && selector.nameLookupPossibleInAdvance()) {
      // Employ caching BitSet optimization
      return makeDictionaryEncodedValueMatcherGeneric(selector, StringPredicateDruidPredicateFactory.equalTo(value));
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
        public boolean matches(boolean includeUnknown)
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return includeUnknown || matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              final int rowId = row.get(i);
              if ((includeUnknown && selector.lookupName(rowId) == null) || rowId == valueId) {
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
      return new ValueMatcher()
      {
        @Override
        public boolean matches(boolean includeUnknown)
        {
          if (includeUnknown || matchNull) {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              return true;
            }
            boolean nullRow = true;
            for (int i = 0; i < size; i++) {
              String rowValue = selector.lookupName(row.get(i));
              if (rowValue == null) {
                return true;
              }
              nullRow = false;
            }
            return nullRow;
          }
          return false;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
        }
      };
    }
  }

  private static ValueMatcher makeNonDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      final @Nullable String value
  )
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return includeUnknown || value == null;
        } else {
          for (int i = 0; i < size; ++i) {
            final String rowValue = selector.lookupName(row.get(i));
            if ((includeUnknown && rowValue == null) || Objects.equals(rowValue, value)) {
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
   * Generic implementation of {@link DimensionSelector#makeValueMatcher(DruidPredicateFactory)}, uses
   * {@link DimensionSelector#getRow()} of the given {@link DimensionSelector}. "Lazy" DimensionSelectors could delegate
   * {@code makeValueMatcher()} to this method, but encouraged to implement {@code makeValueMatcher()} themselves,
   * bypassing the {@link IndexedInts} abstraction.
   */
  public static ValueMatcher makeValueMatcherGeneric(DimensionSelector selector, DruidPredicateFactory predicateFactory)
  {
    int cardinality = selector.getValueCardinality();
    if (cardinality >= 0 && selector.nameLookupPossibleInAdvance()) {
      return makeDictionaryEncodedValueMatcherGeneric(selector, predicateFactory);
    } else {
      return makeNonDictionaryEncodedValueMatcherGeneric(selector, predicateFactory);
    }
  }

  private static ValueMatcher makeDictionaryEncodedValueMatcherGeneric(
      final DimensionSelector selector,
      DruidPredicateFactory predicateFactory
  )
  {
    final BitSet checkedIds = new BitSet(selector.getValueCardinality());
    final BitSet matchingIds = new BitSet(selector.getValueCardinality());
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();

    // Lazy matcher; only check an id if matches() is called.
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return predicate.apply(null).matches(includeUnknown);
        } else {
          for (int i = 0; i < size; ++i) {
            final int id = row.get(i);
            final boolean matches;

            if (checkedIds.get(id)) {
              matches = matchingIds.get(id);
            } else {
              final String rowValue = selector.lookupName(id);
              matches = predicate.apply(rowValue).matches(includeUnknown);
              checkedIds.set(id);
              if (matches) {
                matchingIds.set(id);
              }
            }

            if (matches) {
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
      final DruidPredicateFactory predicateFactory
  )
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final IndexedInts row = selector.getRow();
        final int size = row.size();
        if (size == 0) {
          // null should match empty rows in multi-value columns
          return predicate.apply(null).matches(includeUnknown);
        } else {
          for (int i = 0; i < size; ++i) {
            final String rowValue = selector.lookupName(row.get(i));
            if (predicate.apply(rowValue).matches(includeUnknown)) {
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

  public static BitSet makePredicateMatchingSet(DimensionSelector selector, DruidObjectPredicate<String> predicate, boolean includeUnknown)
  {
    if (!selector.nameLookupPossibleInAdvance()) {
      throw new IAE("selector.nameLookupPossibleInAdvance() should return true");
    }
    int cardinality = selector.getValueCardinality();
    BitSet valueIds = new BitSet(cardinality);
    for (int i = 0; i < cardinality; i++) {
      if (predicate.apply(selector.lookupName(i)).matches(includeUnknown)) {
        valueIds.set(i);
      }
    }
    return valueIds;
  }
}
