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

package org.apache.druid.server.system.table;

import org.apache.druid.query.Query;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Declares a pushdown-capable system-table column and its optional supplier-side name.
 */
public record SystemTablePushdownFilter(String key, @Nullable String value)
{
  public static List<DimFilter> extract(
      final Query<?> query,
      final List<SystemTablePushdownFilter> pushdownFilters
  )
  {
    if (pushdownFilters.isEmpty()) {
      return Collections.emptyList();
    }
    final Map<String, String> columnMappings = pushdownFilters.stream().collect(
        Collectors.toMap(
            SystemTablePushdownFilter::key,
            filter -> filter.value() == null ? filter.key() : filter.value()
        )
    );

    final List<DimFilter> extracted = new ArrayList<>();
    extractConjuncts(query.getFilter(), columnMappings, extracted);
    if (query instanceof WindowOperatorQuery) {
      for (final OperatorFactory leafOperator : ((WindowOperatorQuery) query).getLeafOperators()) {
        if (leafOperator instanceof ScanOperatorFactory) {
          extractConjuncts(((ScanOperatorFactory) leafOperator).getFilter(), columnMappings, extracted);
        }
      }
    }
    return Collections.unmodifiableList(extracted);
  }

  private static void extractConjuncts(
      @Nullable final DimFilter filter,
      final Map<String, String> columnMappings,
      final List<DimFilter> extracted
  )
  {
    if (filter instanceof AndDimFilter) {
      for (final DimFilter subfilter : ((AndDimFilter) filter).getFields()) {
        extractConjuncts(subfilter, columnMappings, extracted);
      }
    } else if (filter != null) {
      final DimFilter rewrittenFilter = rewriteSupportedFilter(filter, columnMappings);
      if (rewrittenFilter != null) {
        extracted.add(rewrittenFilter);
      }
    }
  }

  @Nullable
  private static DimFilter rewriteSupportedFilter(
      final DimFilter filter,
      final Map<String, String> columnMappings
  )
  {
    return switch (filter) {
      case SelectorDimFilter selector -> {
        final String mappedColumn = columnMappings.get(selector.getDimension());
        yield mappedColumn != null && selector.getValue() != null && selector.getExtractionFn() == null
              ? new SelectorDimFilter(mappedColumn, selector.getValue(), null)
              : null;
      }
      case EqualityFilter equality -> {
        final String mappedColumn = columnMappings.get(equality.getColumn());
        yield mappedColumn != null
              && ColumnType.STRING.equals(equality.getMatchValueType())
              && equality.getMatchValue() instanceof String
              ? new EqualityFilter(mappedColumn, ColumnType.STRING, equality.getMatchValue(), null)
              : null;
      }
      case InDimFilter in -> {
        final String mappedColumn = columnMappings.get(in.getDimension());
        yield mappedColumn != null
              && in.getExtractionFn() == null
              && !in.getValues().isEmpty()
              && in.getValues().stream().allMatch(Objects::nonNull)
              ? new InDimFilter(mappedColumn, in.getValues(), null)
              : null;
      }
      case TypedInFilter in -> {
        final String mappedColumn = columnMappings.get(in.getColumn());
        yield mappedColumn != null
              && ColumnType.STRING.equals(in.getMatchValueType())
              && !in.getSortedValues().isEmpty()
              && in.getSortedValues().stream().allMatch(String.class::isInstance)
              ? new TypedInFilter(mappedColumn, ColumnType.STRING, null, in.getSortedValues(), null)
              : null;
      }
      case OrDimFilter or -> {
        final List<DimFilter> rewrittenFields = new ArrayList<>();
        boolean supported = true;
        for (final DimFilter field : or.getFields()) {
          final DimFilter rewrittenField = rewriteSupportedFilter(field, columnMappings);
          if (rewrittenField == null || !isStringValuesFilter(rewrittenField)) {
            supported = false;
            break;
          }
          rewrittenFields.add(rewrittenField);
        }
        yield supported && !rewrittenFields.isEmpty() && hasSingleColumn(rewrittenFields)
              ? new OrDimFilter(rewrittenFields)
              : null;
      }
      case LikeDimFilter like -> {
        final String mappedColumn = columnMappings.get(like.getDimension());
        yield mappedColumn != null && like.getExtractionFn() == null && like.getEscape() == null
              ? new LikeDimFilter(mappedColumn, like.getPattern(), null, null)
              : null;
      }
      case NotDimFilter not -> {
        final DimFilter rewrittenField = rewriteSupportedFilter(not.getField(), columnMappings);
        yield rewrittenField != null
              && (isStringValuesFilter(rewrittenField) || rewrittenField instanceof LikeDimFilter)
              ? new NotDimFilter(rewrittenField)
              : null;
      }
      case BoundDimFilter bound -> {
        final String mappedColumn = columnMappings.get(bound.getDimension());
        yield mappedColumn != null
              && bound.getExtractionFn() == null
              && StringComparators.LEXICOGRAPHIC.equals(bound.getOrdering())
              ? new BoundDimFilter(
                  mappedColumn,
                  bound.getLower(),
                  bound.getUpper(),
                  bound.isLowerStrict(),
                  bound.isUpperStrict(),
                  null,
                  null,
                  StringComparators.LEXICOGRAPHIC
              )
              : null;
      }
      case RangeFilter range -> {
        final String mappedColumn = columnMappings.get(range.getColumn());
        yield mappedColumn != null
              && ColumnType.STRING.equals(range.getMatchValueType())
              && (range.getLower() == null || range.getLower() instanceof String)
              && (range.getUpper() == null || range.getUpper() instanceof String)
              ? new RangeFilter(
                  mappedColumn,
                  ColumnType.STRING,
                  range.getLower(),
                  range.getUpper(),
                  range.isLowerOpen(),
                  range.isUpperOpen(),
                  null
              )
              : null;
      }
      default -> null;
    };
  }

  private static boolean isStringValuesFilter(final DimFilter filter)
  {
    return filter instanceof SelectorDimFilter
           || filter instanceof EqualityFilter
           || filter instanceof InDimFilter
           || filter instanceof TypedInFilter
           || filter instanceof OrDimFilter;
  }

  private static boolean hasSingleColumn(final List<DimFilter> filters)
  {
    final String column = getStringValuesColumn(filters.get(0));
    return filters.stream().allMatch(filter -> column.equals(getStringValuesColumn(filter)));
  }

  public static String getStringValuesColumn(final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> selector.getDimension();
      case EqualityFilter equality -> equality.getColumn();
      case InDimFilter in -> in.getDimension();
      case TypedInFilter in -> in.getColumn();
      case null, default -> getStringValuesColumn(((OrDimFilter) filter).getFields().get(0));
    };
  }

  /** Returns the string values from a validated string-values pushdown filter. */
  public static Set<String> getStringValues(final DimFilter filter)
  {
    return switch (filter) {
      case SelectorDimFilter selector -> Set.of(selector.getValue());
      case EqualityFilter equality -> Set.of((String) equality.getMatchValue());
      case InDimFilter in -> in.getValues();
      case TypedInFilter in -> in.getSortedValues().stream().map(String.class::cast).collect(Collectors.toSet());
      case null, default -> {
        final Set<String> values = new HashSet<>();
        for (final DimFilter field : ((OrDimFilter) filter).getFields()) {
          values.addAll(getStringValues(field));
        }
        yield values;
      }
    };
  }
}
