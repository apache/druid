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

package org.apache.druid.metadata;

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;

import java.util.Objects;

/**
 * Metadata-database capabilities for translating validated task-storage filters into SQL predicates.
 *
 * <p>Every supported translation must return a superset of the rows matched by the corresponding Druid filter.
 * The original Druid filter remains on the Broker query and removes any false positives. A dialect must decline a
 * translation if its database comparison, collation, or null semantics could introduce false negatives.</p>
 */
public enum SqlDialect
{
  NONE(false),
  DERBY(true),
  MYSQL(true),
  POSTGRESQL(true),
  SQL_SERVER(true);

  private final boolean supportsEquality;

  SqlDialect(final boolean supportsEquality)
  {
    this.supportsEquality = supportsEquality;
  }

  /**
   * Equality and {@code IN} predicates use bound string parameters and are safe candidate filters for the configured
   * metadata stores.
   */
  public boolean supports(final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> supportsEquality
                                         && selector.getValue() != null
                                         && selector.getExtractionFn() == null;
      case EqualityFilter equality -> supportsEquality
                                      && ColumnType.STRING.equals(equality.getMatchValueType())
                                      && equality.getMatchValue() instanceof String;
      case InDimFilter in -> supportsEquality
                             && in.getExtractionFn() == null
                             && !in.getValues().isEmpty()
                             && in.getValues().stream().allMatch(Objects::nonNull);
      case TypedInFilter in -> supportsEquality
                               && ColumnType.STRING.equals(in.getMatchValueType())
                               && !in.getSortedValues().isEmpty()
                               && in.getSortedValues().stream().allMatch(String.class::isInstance);
      case OrDimFilter or -> !or.getFields().isEmpty() && or.getFields().stream().allMatch(this::supports);
      default -> false;
    };
  }
}
