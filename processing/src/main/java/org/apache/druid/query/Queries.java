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

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PublicApi
public class Queries
{
  public static List<PostAggregator> decoratePostAggregators(
      List<PostAggregator> postAggs,
      Map<String, AggregatorFactory> aggFactories
  )
  {
    List<PostAggregator> decorated = Lists.newArrayListWithExpectedSize(postAggs.size());
    for (PostAggregator aggregator : postAggs) {
      decorated.add(aggregator.decorate(aggFactories));
    }
    return decorated;
  }

  /**
   * Like {@link #prepareAggregations(List, List, List)} but with otherOutputNames as an empty list. Deprecated
   * because it makes it easy to forget to include dimensions, etc. in "otherOutputNames".
   *
   * @param aggFactories aggregator factories for this query
   * @param postAggs     post-aggregators for this query
   *
   * @return decorated post-aggregators
   *
   * @throws NullPointerException     if aggFactories is null
   * @throws IllegalArgumentException if there are any output name collisions or missing post-aggregator inputs
   */
  @Deprecated
  public static List<PostAggregator> prepareAggregations(
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    return prepareAggregations(Collections.emptyList(), aggFactories, postAggs);
  }

  /**
   * Returns decorated post-aggregators, based on original un-decorated post-aggregators. In addition, this method
   * also verifies that there are no output name collisions, and that all of the post-aggregators' required input
   * fields are present.
   *
   * @param otherOutputNames names of fields that will appear in the same output namespace as aggregators and
   *                         post-aggregators, and are also assumed to be valid inputs to post-aggregators. For most
   *                         built-in query types, this is either empty, or the list of dimension output names.
   * @param aggFactories     aggregator factories for this query
   * @param postAggs         post-aggregators for this query
   *
   * @return decorated post-aggregators
   *
   * @throws NullPointerException     if otherOutputNames or aggFactories is null
   * @throws IllegalArgumentException if there are any output name collisions or missing post-aggregator inputs
   */
  public static List<PostAggregator> prepareAggregations(
      List<String> otherOutputNames,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    Preconditions.checkNotNull(otherOutputNames, "otherOutputNames cannot be null");
    Preconditions.checkNotNull(aggFactories, "aggregations cannot be null");

    final Set<String> combinedOutputNames = new HashSet<>(otherOutputNames);

    final Map<String, AggregatorFactory> aggsFactoryMap = new HashMap<>();
    for (AggregatorFactory aggFactory : aggFactories) {
      Preconditions.checkArgument(
          combinedOutputNames.add(aggFactory.getName()),
          "[%s] already defined", aggFactory.getName()
      );
      aggsFactoryMap.put(aggFactory.getName(), aggFactory);
    }

    if (postAggs != null && !postAggs.isEmpty()) {
      List<PostAggregator> decorated = Lists.newArrayListWithExpectedSize(postAggs.size());
      for (final PostAggregator postAgg : postAggs) {
        final Set<String> dependencies = postAgg.getDependentFields();
        final Set<String> missing = Sets.difference(dependencies, combinedOutputNames);

        Preconditions.checkArgument(
            missing.isEmpty(),
            "Missing fields [%s] for postAggregator [%s]", missing, postAgg.getName()
        );
        Preconditions.checkArgument(
            combinedOutputNames.add(postAgg.getName()),
            "[%s] already defined", postAgg.getName()
        );

        decorated.add(postAgg.decorate(aggsFactoryMap));
      }
      return decorated;
    }

    return postAggs;
  }

  /**
   * Rewrite "query" to refer to some specific segment descriptors.
   *
   * The dataSource for "query" must be based on a single table for this operation to be valid. Otherwise, this
   * function will throw an exception.
   *
   * Unlike the seemingly-similar {@code query.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(descriptors))},
   * this this method will walk down subqueries found within the query datasource, if any, and modify the lowest-level
   * subquery. The effect is that
   * {@code DataSourceAnalysis.forDataSource(query.getDataSource()).getBaseQuerySegmentSpec()} is guaranteed to return
   * either {@code new MultipleSpecificSegmentSpec(descriptors)} or empty.
   *
   * Because {@link BaseQuery#getRunner} is implemented using {@link DataSourceAnalysis#getBaseQuerySegmentSpec}, this
   * method will cause the runner to be a specific-segments runner.
   */
  public static <T> Query<T> withSpecificSegments(final Query<T> query, final List<SegmentDescriptor> descriptors)
  {
    final Query<T> retVal;

    if (query.getDataSource() instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) query.getDataSource()).getQuery();
      retVal = query.withDataSource(new QueryDataSource(withSpecificSegments(subQuery, descriptors)));
    } else {
      retVal = query.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(descriptors));
    }

    // Verify preconditions and invariants, just in case.
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(retVal.getDataSource());

    // Sanity check: query must be based on a single table.
    if (!analysis.getBaseTableDataSource().isPresent()) {
      throw new ISE("Unable to apply specific segments to non-table-based dataSource[%s]", query.getDataSource());
    }

    if (analysis.getBaseQuerySegmentSpec().isPresent()
        && !analysis.getBaseQuerySegmentSpec().get().equals(new MultipleSpecificSegmentSpec(descriptors))) {
      // If you see the error message below, it's a bug in either this function or in DataSourceAnalysis.
      throw new ISE("Unable to apply specific segments to query with dataSource[%s]", query.getDataSource());
    }

    return retVal;
  }

  /**
   * Rewrite "query" to refer to some specific base datasource, instead of the one it currently refers to.
   *
   * Unlike the seemingly-similar {@link Query#withDataSource}, this will walk down the datasource tree and replace
   * only the base datasource (in the sense defined in {@link DataSourceAnalysis}).
   */
  public static <T> Query<T> withBaseDataSource(final Query<T> query, final DataSource newBaseDataSource)
  {
    final Query<T> retVal;

    if (query.getDataSource() instanceof QueryDataSource) {
      final Query<?> subQuery = ((QueryDataSource) query.getDataSource()).getQuery();
      retVal = query.withDataSource(new QueryDataSource(withBaseDataSource(subQuery, newBaseDataSource)));
    } else {
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

      DataSource current = newBaseDataSource;
      DimFilter joinBaseFilter = analysis.getJoinBaseTableFilter().orElse(null);

      for (final PreJoinableClause clause : analysis.getPreJoinableClauses()) {
        current = JoinDataSource.create(
            current,
            clause.getDataSource(),
            clause.getPrefix(),
            clause.getCondition(),
            clause.getJoinType(),
            joinBaseFilter
        );
        joinBaseFilter = null;
      }

      retVal = query.withDataSource(current);
    }

    // Verify postconditions, just in case.
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(retVal.getDataSource());

    if (!newBaseDataSource.equals(analysis.getBaseDataSource())) {
      throw new ISE("Unable to replace base dataSource");
    }

    return retVal;
  }

  /**
   * Helper for implementations of {@link Query#getRequiredColumns()}. Returns the list of columns that will be read
   * out of a datasource by a query that uses the provided objects in the usual way.
   *
   * The returned set always contains {@code __time}, no matter what.
   *
   * If the virtual columns, filter, dimensions, aggregators, or additional columns refer to a virtual column, then the
   * inputs of the virtual column will be returned instead of the name of the virtual column itself. Therefore, the
   * returned list will never contain the names of any virtual columns.
   *
   * @param virtualColumns    virtual columns whose inputs should be included.
   * @param filter            optional filter whose inputs should be included.
   * @param dimensions        dimension specs whose inputs should be included.
   * @param aggregators       aggregators whose inputs should be included.
   * @param additionalColumns additional columns to include. Each of these will be added to the returned set, unless it
   *                          refers to a virtual column, in which case the virtual column inputs will be added instead.
   */
  public static Set<String> computeRequiredColumns(
      final VirtualColumns virtualColumns,
      @Nullable final DimFilter filter,
      final List<DimensionSpec> dimensions,
      final List<AggregatorFactory> aggregators,
      final List<String> additionalColumns
  )
  {
    final Set<String> requiredColumns = new HashSet<>();

    // Everyone needs __time (it's used by intervals filters).
    requiredColumns.add(ColumnHolder.TIME_COLUMN_NAME);

    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      for (String column : virtualColumn.requiredColumns()) {
        if (!virtualColumns.exists(column)) {
          requiredColumns.addAll(virtualColumn.requiredColumns());
        }
      }
    }

    if (filter != null) {
      for (String column : filter.getRequiredColumns()) {
        if (!virtualColumns.exists(column)) {
          requiredColumns.add(column);
        }
      }
    }

    for (DimensionSpec dimensionSpec : dimensions) {
      if (!virtualColumns.exists(dimensionSpec.getDimension())) {
        requiredColumns.add(dimensionSpec.getDimension());
      }
    }

    for (AggregatorFactory aggregator : aggregators) {
      for (String column : aggregator.requiredFields()) {
        if (!virtualColumns.exists(column)) {
          requiredColumns.add(column);
        }
      }
    }

    for (String column : additionalColumns) {
      if (!virtualColumns.exists(column)) {
        requiredColumns.add(column);
      }
    }

    return requiredColumns;
  }

  public static <T> Query<T> withMaxScatterGatherBytes(Query<T> query, long maxScatterGatherBytesLimit)
  {
    QueryContext context = query.context();
    if (!context.containsKey(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY)) {
      return query.withOverriddenContext(ImmutableMap.of(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, maxScatterGatherBytesLimit));
    }
    context.verifyMaxScatterGatherBytes(maxScatterGatherBytesLimit);
    return query;
  }

  public static <T> Query<T> withTimeout(Query<T> query, long timeout)
  {
    return query.withOverriddenContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, timeout));
  }

  public static <T> Query<T> withDefaultTimeout(Query<T> query, long defaultTimeout)
  {
    return query.withOverriddenContext(ImmutableMap.of(QueryContexts.DEFAULT_TIMEOUT_KEY, defaultTimeout));
  }
}
