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
import io.druid.segment.filter.BooleanValueMatcher;

public class StringValueMatcherColumnSelectorStrategy implements ValueMatcherColumnSelectorStrategy<DimensionSelector>
{
  @Override
  public ValueMatcher makeValueMatcher(final DimensionSelector selector, final String value)
  {
    final String valueStr = Strings.emptyToNull(value);

    // if matching against null, rows with size 0 should also match
    final boolean matchNull = Strings.isNullOrEmpty(valueStr);

    final int cardinality = selector.getValueCardinality();

    if (cardinality == 0 || (cardinality == 1 && selector.lookupName(0) == null)) {
      // All values are null or empty rows (which match nulls anyway). No need to check each row.
      return BooleanValueMatcher.of(matchNull);
    } else {
      return selector.makeValueMatcher(valueStr, matchNull);
    }
  }

  @Override
  public ValueMatcher makeValueMatcher(
      final DimensionSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final int cardinality = selector.getValueCardinality();
    final boolean matchNull = predicate.apply(null);

    if (cardinality == 0 || (cardinality == 1 && selector.lookupName(0) == null)) {
      // All values are null or empty rows (which match nulls anyway). No need to check each row.
      return BooleanValueMatcher.of(matchNull);
    } else {
      return selector.makeValueMatcher(predicate, matchNull);
    }
  }

}
