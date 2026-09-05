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
import org.apache.druid.server.system.table.SystemTablePushdownFilter;
import org.skife.jdbi.v2.Query;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts validated task storage filters to a SQL predicate and its corresponding bindings.
 *
 * <p>SQL generation and parameter binding intentionally happen together for each filter type so that a generated
 * placeholder cannot get out of sync with the bindings applied to the query.</p>
 */
final class SqlPredicateBuilder
{
  private final StringBuilder sql = new StringBuilder();
  private final Map<String, Object> bindings = new LinkedHashMap<>();

  SqlPredicateBuilder(
      @Nullable final String dataSource,
      final List<DimFilter> filters,
      final SqlDialect dialect
  )
  {
    if (dataSource != null) {
      appendStringValues("datasource", "legacy_datasource_filter", List.of(dataSource), false);
    }
    for (int i = 0; i < filters.size(); i++) {
      final DimFilter filter = filters.get(i);
      // Planning extracts possible storage prefilters without removing the original Druid filter from either the
      // component Scan or the Broker query. Skipping a dialect-unsupported predicate therefore returns more candidate
      // rows but cannot change the final result.
      if (dialect.supports(filter)) {
        appendStringValuesFilter(filter, "task_filter_" + i);
      }
    }
  }

  String getSql()
  {
    return sql.toString();
  }

  <T> Query<T> bind(Query<T> query)
  {
    for (final Map.Entry<String, Object> binding : bindings.entrySet()) {
      query = query.bind(binding.getKey(), binding.getValue());
    }
    return query;
  }

  private void appendStringValuesFilter(final DimFilter filter, final String parameterPrefix)
  {
    final String column = SystemTablePushdownFilter.getStringValuesColumn(filter);
    appendStringValues(
        column,
        parameterPrefix,
        SystemTablePushdownFilter.getStringValues(filter).stream().sorted().toList(),
        isTaskMigrationNullableColumn(column)
    );
  }

  private void appendStringValues(
      final String column,
      final String parameterPrefix,
      final List<String> values,
      final boolean includeNull
  )
  {
    sql.append(" AND ");
    if (includeNull) {
      sql.append('(');
    }
    sql.append(column);
    if (values.size() == 1) {
      sql.append(" = :").append(parameterPrefix).append("_0");
    } else {
      sql.append(" IN (");
      appendParameters(parameterPrefix, values);
      sql.append(')');
    }
    if (includeNull) {
      sql.append(" OR ").append(column).append(" IS NULL)");
    }
    sql.append(' ');
    addBindings(parameterPrefix, values);
  }

  private void appendParameters(final String parameterPrefix, final List<String> values)
  {
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(':').append(parameterPrefix).append('_').append(i);
    }
  }

  private void addBindings(final String parameterPrefix, final List<String> values)
  {
    for (int i = 0; i < values.size(); i++) {
      bindings.put(parameterPrefix + '_' + i, values.get(i));
    }
  }

  private static boolean isTaskMigrationNullableColumn(final String column)
  {
    return "group_id".equals(column) || "type".equals(column);
  }
}
