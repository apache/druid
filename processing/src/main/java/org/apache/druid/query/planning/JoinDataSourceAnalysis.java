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

package org.apache.druid.query.planning;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.join.JoinPrefixUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Analysis of a datasource for purposes of deciding how to execute a particular query.
 *
 * The analysis breaks a datasource down in the following way:
 *
 * <pre>
 *
 *                             Q  <-- Possible query datasource(s) [may be none, or multiple stacked]
 *                             |
 *                             Q  <-- Base query datasource, returned by {@link #getBaseQuery()} if it exists
 *                             |
 *                             J  <-- Possible join tree, expected to be left-leaning
 *                            / \
 *                           J  Dj <--  Other leaf datasources
 *   Base datasource        / \         which will be joined
 *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
 *
 * </pre>
 *
 * The base datasource (Db) is returned by {@link #getBaseDataSource()}. The other leaf datasources are returned by
 * {@link #getPreJoinableClauses()}.
 *
 * The base datasource (Db) will never be a join, but it can be any other type of datasource (table, query, etc).
 * Note that join trees are only flattened if they occur at the top of the overall tree (or underneath an outer query),
 * and that join trees are only flattened to the degree that they are left-leaning. Due to these facts, it is possible
 * for the base or leaf datasources to include additional joins.
 *
 * The base datasource is the one that will be considered by the core Druid query stack for scanning via
 * {@link org.apache.druid.segment.Segment} and {@link org.apache.druid.segment.CursorFactory}. The other leaf
 * datasources must be joinable onto the base data.
 *
 * The idea here is to keep things simple and dumb. So we focus only on identifying left-leaning join trees, which map
 * neatly onto a series of hash table lookups at query time. The user/system generating the queries, e.g. the druid-sql
 * layer (or the end user in the case of native queries), is responsible for containing the smarts to structure the
 * tree in a way that will lead to optimal execution.
 */
public class JoinDataSourceAnalysis
{
  private final DataSource baseDataSource;
  @Nullable
  private final DimFilter joinBaseTableFilter;
  private final List<PreJoinableClause> preJoinableClauses;

  public JoinDataSourceAnalysis(
      DataSource baseDataSource,
      @Nullable Query<?> baseQuery,
      @Nullable DimFilter joinBaseTableFilter,
      List<PreJoinableClause> preJoinableClauses,
      @Nullable
      QuerySegmentSpec querySegmentSpec
  )
  {
    if (baseDataSource instanceof JoinDataSource) {
      // The base cannot be a join (this is a class invariant).
      // If it happens, it's a bug in the datasource analyzer.
      throw new IAE("Base dataSource cannot be a join! Original base datasource was: %s", baseDataSource);
    }

    this.baseDataSource = baseDataSource;
    this.joinBaseTableFilter = joinBaseTableFilter;
    this.preJoinableClauses = preJoinableClauses;
  }

  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * If the original data source is a join data source and there is a DimFilter on the base table data source,
   * that DimFilter is returned here
   */
  public Optional<DimFilter> getJoinBaseTableFilter()
  {
    return Optional.ofNullable(joinBaseTableFilter);
  }

  /**
   * Returns join clauses corresponding to joinable leaf datasources (every leaf except the bottom-leftmost).
   */
  public List<PreJoinableClause> getPreJoinableClauses()
  {
    return preJoinableClauses;
  }
  /**
   * Returns whether "column" on the analyzed datasource refers to a column from the base datasource.
   */
  public boolean isBaseColumn(final String column)
  {
    for (final PreJoinableClause clause : preJoinableClauses) {
      if (JoinPrefixUtils.isPrefixedBy(column, clause.getPrefix())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinDataSourceAnalysis that = (JoinDataSourceAnalysis) o;
    return Objects.equals(baseDataSource, that.baseDataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDataSource);
  }

  @Override
  public String toString()
  {
    return "DataSourceAnalysis{" +
           ", baseDataSource=" + baseDataSource +
           ", preJoinableClauses=" + preJoinableClauses +
           '}';
  }

  public static JoinDataSourceAnalysis constructAnalysis(final JoinDataSource dataSource)
  {
    DataSource current = dataSource;
    DimFilter currentDimFilter = TrueDimFilter.instance();
    final List<PreJoinableClause> preJoinableClauses = new ArrayList<>();

    do {
      if (current instanceof JoinDataSource) {
        final JoinDataSource joinDataSource = (JoinDataSource) current;
        currentDimFilter = DimFilters.conjunction(currentDimFilter, joinDataSource.getLeftFilter());
        PreJoinableClause e = new PreJoinableClause(joinDataSource);
        preJoinableClauses.add(e);
        current = joinDataSource.getLeft();
        continue;
      }
      break;
    } while (true);

    if (currentDimFilter == TrueDimFilter.instance()) {
      currentDimFilter = null;
    }

    // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
    // going-up order. So reverse them.
    Collections.reverse(preJoinableClauses);

    return new JoinDataSourceAnalysis(current, null, currentDimFilter, preJoinableClauses, null);
  }
}
