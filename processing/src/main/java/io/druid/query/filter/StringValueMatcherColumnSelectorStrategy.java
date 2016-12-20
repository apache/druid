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

package io.druid.query.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.BitSet;
import java.util.Objects;

public class StringValueMatcherColumnSelectorStrategy implements ValueMatcherColumnSelectorStrategy
{
  @Override
  public ValueMatcher getValueMatcher(String columnName, ColumnSelectorFactory cursor, final String value)
  {
    final String valueStr = Strings.emptyToNull(value);
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(columnName, columnName)
    );

    // if matching against null, rows with size 0 should also match
    final boolean matchNull = Strings.isNullOrEmpty(valueStr);

    final int cardinality = selector.getValueCardinality();

    if (cardinality >= 0) {
      // Dictionary-encoded dimension. Compare by id instead of by value to save time.
      final int valueId = selector.lookupId(valueStr);

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
      // Not dictionary-encoded. Skip the optimization.
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
              if (Objects.equals(selector.lookupName(row.get(i)), valueStr)) {
                return true;
              }
            }
            return false;
          }
        }
      };
    }
  }

  @Override
  public ValueMatcher getValueMatcher(String columnName, ColumnSelectorFactory cursor, final DruidPredicateFactory predicateFactory)
  {
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(columnName, columnName)
    );

    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final int cardinality = selector.getValueCardinality();
    final boolean matchNull = predicate.apply(null);

    if (cardinality >= 0) {
      // Dictionary-encoded dimension. Check every value; build a bitset of matching ids.
      final BitSet valueIds = new BitSet(cardinality);
      for (int i = 0; i < cardinality; i++) {
        if (predicate.apply(selector.lookupName(i))) {
          valueIds.set(i);
        }
      }

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
              if (valueIds.get(row.get(i))) {
                return true;
              }
            }
            return false;
          }
        }
      };
    } else {
      // Not dictionary-encoded. Skip the optimization.
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
  }
}
