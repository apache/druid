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

import org.apache.druid.segment.ColumnSelectorFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public interface BooleanFilter extends Filter
{
  ValueMatcher[] EMPTY_VALUE_MATCHER_ARRAY = new ValueMatcher[0];

  /**
   * Returns the child filters for this filter.
   *
   * This is a LinkedHashSet because we don't want duplicates, but the order is also important in some cases (such
   * as when filters are provided in an order that encourages short-circuiting.)
   */
  LinkedHashSet<Filter> getFilters();

  /**
   * Get a ValueMatcher that applies this filter to row values.
   *
   * Unlike makeMatcher(ValueMatcherFactory), this method allows the Filter to utilize bitmap indexes.
   *
   * An implementation should either:
   * - return a ValueMatcher that checks row values, using the provided ValueMatcherFactory
   * - or, if possible, get a bitmap index for this filter using the ColumnIndexSelector, and
   * return a ValueMatcher that checks the current row offset, created using the bitmap index.
   *
   * @param selector                Object used to retrieve bitmap indexes
   * @param columnSelectorFactory   Object used to select columns for making ValueMatchers
   * @param rowOffsetMatcherFactory Object used to create RowOffsetMatchers
   *
   * @return ValueMatcher that applies this filter
   */
  ValueMatcher makeMatcher(
      ColumnIndexSelector selector,
      ColumnSelectorFactory columnSelectorFactory,
      RowOffsetMatcherFactory rowOffsetMatcherFactory
  );

  @Override
  default Set<String> getRequiredColumns()
  {
    Set<String> allColumns = new HashSet<>();
    for (Filter f : getFilters()) {
      allColumns.addAll(f.getRequiredColumns());
    }
    return allColumns;
  }

}
