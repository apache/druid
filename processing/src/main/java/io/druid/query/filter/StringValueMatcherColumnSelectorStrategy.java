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
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;

public class StringValueMatcherColumnSelectorStrategy implements ValueMatcherColumnSelectorStrategy<DimensionSelector>
{
  private static final String[] NULL_VALUE = new String[]{ null };
  private static final ValueGetter NULL_VALUE_GETTER = new ValueGetter()
  {
    @Override
    public String[] get()
    {
      return NULL_VALUE;
    }
  };

  @Override
  public ValueMatcher makeValueMatcher(final DimensionSelector selector, String value)
  {
    value = Strings.emptyToNull(value);
    if (selector.getValueCardinality() == 0) {
      return BooleanValueMatcher.of(value == null);
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
    if (selector.getValueCardinality() == 0) {
      return BooleanValueMatcher.of(predicate.apply(null));
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
      return new ValueGetter()
      {
        @Override
        public String[] get()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            return NULL_VALUE;
          } else {
            String[] values = new String[size];
            for (int i = 0; i < size; ++i) {
              values[i] = Strings.emptyToNull(selector.lookupName(row.get(i)));
            }
            return values;
          }
        }
      };
    }
  }
}
