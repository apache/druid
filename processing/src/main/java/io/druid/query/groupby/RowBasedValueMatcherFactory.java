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

package io.druid.query.groupby;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.column.Column;
import io.druid.segment.filter.BooleanValueMatcher;

import java.util.List;
import java.util.Objects;

public class RowBasedValueMatcherFactory implements ValueMatcherFactory
{
  private Row row;

  public void setRow(Row row)
  {
    this.row = row;
  }

  @Override
  public ValueMatcher makeValueMatcher(final String dimension, final String value)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      if (value == null) {
        return new BooleanValueMatcher(false);
      }

      final Long longValue = GuavaUtils.tryParseLong(value);
      if (longValue == null) {
        return new BooleanValueMatcher(false);
      }

      return new ValueMatcher()
      {
        // store the primitive, so we don't unbox for every comparison
        final long unboxedLong = longValue;

        @Override
        public boolean matches()
        {
          return row.getTimestampFromEpoch() == unboxedLong;
        }
      };
    } else {
      final String valueOrNull = Strings.emptyToNull(value);
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return doesMatch(row.getRaw(dimension), valueOrNull);
        }
      };
    }
  }

  // There is no easy way to determine column types from a Row, so this generates all possible predicates and then
  // uses instanceof checks to determine which one to apply to each row.
  @Override
  public ValueMatcher makeValueMatcher(final String dimension, final DruidPredicateFactory predicateFactory)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return new ValueMatcher()
      {
        final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();

        @Override
        public boolean matches()
        {
          return predicate.applyLong(row.getTimestampFromEpoch());
        }
      };
    } else {
      return new ValueMatcher()
      {
        final Predicate<String> stringPredicate = predicateFactory.makeStringPredicate();
        final DruidLongPredicate longPredicate = predicateFactory.makeLongPredicate();

        @Override
        public boolean matches()
        {
          return doesMatch(row.getRaw(dimension), stringPredicate, longPredicate);
        }
      };
    }
  }

  // Precondition: value must be run through Strings.emptyToNull
  private boolean doesMatch(final Object raw, final String value)
  {
    if (raw == null) {
      return value == null;
    } else if (raw instanceof List) {
      final List theList = (List) raw;
      if (theList.isEmpty()) {
        // null should match empty rows in multi-value columns
        return value == null;
      } else {
        for (Object o : theList) {
          if (doesMatch(o, value)) {
            return true;
          }
        }

        return false;
      }
    } else {
      return Objects.equals(Strings.emptyToNull(raw.toString()), value);
    }
  }

  private boolean doesMatch(
      final Object raw,
      final Predicate<String> stringPredicate,
      final DruidLongPredicate longPredicate
  )
  {
    if (raw == null) {
      return stringPredicate.apply(null);
    } else if (raw instanceof List) {
      final List theList = (List) raw;
      if (theList.isEmpty()) {
        return stringPredicate.apply(null);
      } else {
        for (Object o : theList) {
          if (doesMatch(o, stringPredicate, longPredicate)) {
            return true;
          }
        }

        return false;
      }
    } else if (raw instanceof Long || raw instanceof Integer) {
      return longPredicate.applyLong(((Number) raw).longValue());
    } else {
      return stringPredicate.apply(Strings.emptyToNull(raw.toString()));
    }
  }
}
