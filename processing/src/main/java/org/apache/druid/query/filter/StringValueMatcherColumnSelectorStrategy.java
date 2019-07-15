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

package org.apache.druid.query.filter;

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringValueMatcherColumnSelectorStrategy implements ValueMatcherColumnSelectorStrategy<DimensionSelector>
{
  private static final String[] NULL_VALUE = new String[]{null};
  private static final ValueGetter NULL_VALUE_GETTER = () -> NULL_VALUE;

  private final boolean hasMultipleValues;

  public StringValueMatcherColumnSelectorStrategy(final boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
  }

  @Nullable
  public static Boolean toBooleanIfPossible(
      final DimensionDictionarySelector selector,
      final boolean hasMultipleValues,
      final Predicate<String> predicate
  )
  {
    if (selector.getValueCardinality() == 0) {
      // Column has no values (it doesn't exist, or it's all empty arrays).
      // Match if and only if "predicate" matches null.
      return predicate.apply(null);
    } else if (!hasMultipleValues && selector.getValueCardinality() == 1 && selector.nameLookupPossibleInAdvance()) {
      // Every row has the same value. Match if and only if "predicate" matches the possible value.
      return predicate.apply(selector.lookupName(0));
    } else {
      return null;
    }
  }

  @Nullable
  private static ValueMatcher toBooleanMatcherIfPossible(
      final DimensionSelector selector,
      final boolean hasMultipleValues,
      final Predicate<String> predicate
  )
  {
    final Boolean booleanValue = StringValueMatcherColumnSelectorStrategy.toBooleanIfPossible(
        selector,
        hasMultipleValues,
        predicate
    );
    return booleanValue == null ? null : BooleanValueMatcher.of(booleanValue);
  }

  @Override
  public ValueMatcher makeValueMatcher(final DimensionSelector selector, final String value)
  {
    final ValueMatcher booleanMatcher = toBooleanMatcherIfPossible(
        selector,
        hasMultipleValues,
        s -> Objects.equals(s, NullHandling.emptyToNullIfNeeded(value))
    );

    if (booleanMatcher != null) {
      return booleanMatcher;
    } else {
      return selector.makeValueMatcher(value);
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(
      final DimensionSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final ValueMatcher booleanMatcher = toBooleanMatcherIfPossible(selector, hasMultipleValues, predicate);

    if (booleanMatcher != null) {
      return booleanMatcher;
    } else {
      return selector.makeValueMatcher(predicate);
    }
  }

  @Override
  public ValueGetter makeValueGetter(final DimensionSelector selector)
  {
    if (selector.getValueCardinality() == 0) {
      return NULL_VALUE_GETTER;
    } else {
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
  }
}
