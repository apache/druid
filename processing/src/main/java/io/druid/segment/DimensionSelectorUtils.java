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
import io.druid.java.util.common.IAE;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;

import java.util.BitSet;
import java.util.Objects;

public final class DimensionSelectorUtils
{

  private DimensionSelectorUtils()
  {
  }

  public static ValueMatcher makeRowBasedValueMatcher(
      DimensionSelector selector,
      String value,
      boolean matchNull
  )
  {
    IdLookup idLookup = selector.idLookup();
    if (idLookup != null) {
      return makeDictionaryEncodedRowBasedValueMatcher(selector, idLookup.lookupId(value), matchNull);
    } else {
      return makeNonDictionaryEncodedIndexedIntsBasedValueMatcher(selector, value, matchNull);
    }
  }

  private static ValueMatcher makeDictionaryEncodedRowBasedValueMatcher(
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
        };
      } else {
        return BooleanValueMatcher.of(false);
      }
    }
  }

  private static ValueMatcher makeNonDictionaryEncodedIndexedIntsBasedValueMatcher(
      final DimensionSelector selector,
      final String value,
      final boolean matchNull
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
          return matchNull;
        } else {
          for (int i = 0; i < size; ++i) {
            if (Objects.equals(selector.lookupName(row.get(i)), value)) {
              return true;
            }
          }
          return false;
        }
      }
    };
  }

  public static ValueMatcher makeRowBasedValueMatcher(
      final DimensionSelector selector,
      final Predicate<String> predicate,
      final boolean matchNull
  )
  {
    int cardinality = selector.getValueCardinality();
    if (cardinality >= 0) {
      return makeDictionaryEncodedRowBasedValueMatcher(selector, predicate, matchNull);
    } else {
      return makeNonDictionaryEncodedRowBasedValueMatcher(selector, predicate, matchNull);
    }
  }

  private static ValueMatcher makeDictionaryEncodedRowBasedValueMatcher(
      final DimensionSelector selector,
      Predicate<String> predicate,
      final boolean matchNull
  )
  {
    final BitSet predicateMatchingValueIds = makePredicateMatchingSet(selector, predicate);
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
    };
  }

  private static ValueMatcher makeNonDictionaryEncodedRowBasedValueMatcher(
      final DimensionSelector selector,
      final Predicate<String> predicate,
      final boolean matchNull
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
