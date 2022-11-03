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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a join of two datasources.
 * <p>
 * Logically, this datasource contains the result of:
 * <p>
 * (1) prefixing all right-side columns with "rightPrefix"
 * (2) then, joining the left and (prefixed) right sides using the provided type and condition
 * <p>
 * Any columns from the left-hand side that start with "rightPrefix", and are at least one character longer than
 * the prefix, will be shadowed. It is up to the caller to ensure that no important columns are shadowed by the
 * chosen prefix.
 * <p>
 * When analyzed by {@link DataSourceAnalysis}, the right-hand side of this datasource
 * will become a {@link PreJoinableClause} object.
 */
public class JoinDataSource implements DataSource
{
  private final DataSource left;
  private final DataSource right;
  private final String rightPrefix;
  private final JoinConditionAnalysis conditionAnalysis;
  private final JoinType joinType;
  // An optional filter on the left side if left is direct table access
  @Nullable
  private final DimFilter leftFilter;
  @Nullable
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private static final Logger log = new Logger(JoinDataSource.class);
  private final DataSourceAnalysis analysis;



  private JoinDataSource(
      DataSource left,
      DataSource right,
      String rightPrefix,
      JoinConditionAnalysis conditionAnalysis,
      JoinType joinType,
      @Nullable DimFilter leftFilter,
      @Nullable JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = JoinPrefixUtils.validatePrefix(rightPrefix);
    this.conditionAnalysis = Preconditions.checkNotNull(conditionAnalysis, "conditionAnalysis");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    //TODO: Add support for union data sources
    Preconditions.checkArgument(
        leftFilter == null || left instanceof TableDataSource,
        "left filter is only supported if left data source is direct table access"
    );
    this.leftFilter = leftFilter;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.analysis = DataSourceAnalysis.forDataSource(this);
  }

  /**
   * Create a join dataSource from a string condition.
   */
  @JsonCreator
  public static JoinDataSource create(
      @JsonProperty("left") DataSource left,
      @JsonProperty("right") DataSource right,
      @JsonProperty("rightPrefix") String rightPrefix,
      @JsonProperty("condition") String condition,
      @JsonProperty("joinType") JoinType joinType,
      @Nullable @JsonProperty("leftFilter") DimFilter leftFilter,
      @JacksonInject ExprMacroTable macroTable,
      @Nullable @JacksonInject JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    return new JoinDataSource(
        left,
        right,
        StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
        JoinConditionAnalysis.forExpression(
            Preconditions.checkNotNull(condition, "condition"),
            StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
            macroTable
        ),
        joinType,
        leftFilter,
        joinableFactoryWrapper
    );
  }

  /**
   * Create a join dataSource from an existing {@link JoinConditionAnalysis}.
   */
  public static JoinDataSource create(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinConditionAnalysis conditionAnalysis,
      final JoinType joinType,
      final DimFilter leftFilter,
      @Nullable final JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    return new JoinDataSource(
        left,
        right,
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper
    );
  }


  @Override
  public Set<String> getTableNames()
  {
    final Set<String> names = new HashSet<>();
    names.addAll(left.getTableNames());
    names.addAll(right.getTableNames());
    return names;
  }

  @JsonProperty
  public DataSource getLeft()
  {
    return left;
  }

  @JsonProperty
  public DataSource getRight()
  {
    return right;
  }

  @JsonProperty
  public String getRightPrefix()
  {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition()
  {
    return conditionAnalysis.getOriginalExpression();
  }

  public JoinConditionAnalysis getConditionAnalysis()
  {
    return conditionAnalysis;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public DimFilter getLeftFilter()
  {
    return leftFilter;
  }

  @Nullable
  public JoinableFactoryWrapper getJoinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 2) {
      throw new IAE("Expected [2] children, got [%d]", children.size());
    }

    return new JoinDataSource(
        children.get(0),
        children.get(1),
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper
    );
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return left.isCacheable(isBroker) && right.isCacheable(isBroker);
  }

  @Override
  public boolean isGlobal()
  {
    return left.isGlobal() && right.isGlobal();
  }

  @Override
  public boolean isConcrete()
  {
    return false;
  }

  /**
   * Computes a set of column names for left table expressions in join condition which may already have been defined as
   * a virtual column in the virtual column registry. It helps to remove any extraenous virtual columns created and only
   * use the relevant ones.
   *
   * @return a set of column names which might be virtual columns on left table in join condition
   */
  public Set<String> getVirtualColumnCandidates()
  {
    return getConditionAnalysis().getEquiConditions()
                                 .stream()
                                 .filter(equality -> equality.getLeftExpr() != null)
                                 .map(equality -> equality.getLeftExpr().analyzeInputs().getRequiredBindings())
                                 .flatMap(Set::stream)
                                 .collect(Collectors.toSet());
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
    JoinDataSource that = (JoinDataSource) o;
    return Objects.equals(left, that.left) &&
           Objects.equals(right, that.right) &&
           Objects.equals(rightPrefix, that.rightPrefix) &&
           Objects.equals(conditionAnalysis, that.conditionAnalysis) &&
           Objects.equals(leftFilter, that.leftFilter) &&
           joinType == that.joinType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(left, right, rightPrefix, conditionAnalysis, joinType, leftFilter);
  }

  @Override
  public String toString()
  {
    return "JoinDataSource{" +
           "left=" + left +
           ", right=" + right +
           ", rightPrefix='" + rightPrefix + '\'' +
           ", condition=" + conditionAnalysis +
           ", joinType=" + joinType +
           ", leftFilter=" + leftFilter +
           '}';
  }


  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   *
   * @param baseFilter         Filter to apply before the join takes place
   * @param clauses            Pre-joinable clauses
   * @param cpuTimeAccumulator An accumulator that we will add CPU nanos to; this is part of the function to encourage
   *                           callers to remember to track metrics on CPU time required for creation of Joinables
   * @param query              The query that will be run on the mapped segments. Usually this should be
   *                           {@code analysis.getBaseQuery().orElse(query)}, where "analysis" is a
   *                           {@link DataSourceAnalysis} and "query" is the original
   *                           query from the end user.
   */
  public Function<SegmentReference, SegmentReference> createSegmentMapFn(
      @Nullable final Filter baseFilter,
      final List<PreJoinableClause> clauses,
      final AtomicLong cpuTimeAccumulator,
      final Query<?> query
  )
  {
    // compute column correlations here and RHS correlated values
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> {
          if (clauses.isEmpty()) {
            return Function.identity();
          } else {
            final JoinableClauses joinableClauses = JoinableClauses.createClauses(
                clauses,
                joinableFactoryWrapper.getJoinableFactory()
            );
            final JoinFilterRewriteConfig filterRewriteConfig = JoinFilterRewriteConfig.forQuery(query);

            // Pick off any join clauses that can be converted into filters.
            final Set<String> requiredColumns = query.getRequiredColumns();
            final Filter baseFilterToUse;
            final List<JoinableClause> clausesToUse;

            if (requiredColumns != null && filterRewriteConfig.isEnableRewriteJoinToFilter()) {
              final Pair<List<Filter>, List<JoinableClause>> conversionResult = JoinableFactoryWrapper.convertJoinsToFilters(
                  joinableClauses.getJoinableClauses(),
                  requiredColumns,
                  Ints.checkedCast(Math.min(filterRewriteConfig.getFilterRewriteMaxSize(), Integer.MAX_VALUE))
              );

              baseFilterToUse =
                  Filters.maybeAnd(
                      Lists.newArrayList(
                          Iterables.concat(
                              Collections.singleton(baseFilter),
                              conversionResult.lhs
                          )
                      )
                  ).orElse(null);
              clausesToUse = conversionResult.rhs;
            } else {
              baseFilterToUse = baseFilter;
              clausesToUse = joinableClauses.getJoinableClauses();
            }

            // Analyze remaining join clauses to see if filters on them can be pushed down.
            final JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
                new JoinFilterPreAnalysisKey(
                    filterRewriteConfig,
                    clausesToUse,
                    query.getVirtualColumns(),
                    Filters.maybeAnd(Arrays.asList(baseFilterToUse, Filters.toFilter(query.getFilter())))
                           .orElse(null)
                )
            );

            return baseSegment ->
                new HashJoinSegment(
                    baseSegment,
                    baseFilterToUse,
                    GuavaUtils.firstNonNull(clausesToUse, ImmutableList.of()),
                    joinFilterPreAnalysis
                );
          }
        }
    );
  }


  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTimeAccumulator
  )
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = createSegmentMapFn(
        analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
        analysis.getPreJoinableClauses(),
        cpuTimeAccumulator,
        analysis.getBaseQuery().orElse(query)
    );
    return segmentMapFn;
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    DataSource current = newSource;
    DimFilter joinBaseFilter = analysis.getJoinBaseTableFilter().orElse(null);

    for (final PreJoinableClause clause : analysis.getPreJoinableClauses()) {
      current = JoinDataSource.create(
          current,
          clause.getDataSource(),
          clause.getPrefix(),
          clause.getCondition(),
          clause.getJoinType(),
          joinBaseFilter,
          this.joinableFactoryWrapper
      );
      joinBaseFilter = null;
    }
    return current;
  }

  @Override
  public byte[] getCacheKey()
  {
    final List<PreJoinableClause> clauses = analysis.getPreJoinableClauses();
    if (clauses.isEmpty()) {
      throw new IAE("No join clauses to build the cache key for data source [%s]", analysis.getDataSource());
    }

    final CacheKeyBuilder keyBuilder;
    keyBuilder = new CacheKeyBuilder(JoinableFactoryWrapper.JOIN_OPERATION);
    if (analysis.getJoinBaseTableFilter().isPresent()) {
      keyBuilder.appendCacheable(analysis.getJoinBaseTableFilter().get());
    }
    for (PreJoinableClause clause : clauses) {
      Optional<byte[]> bytes = joinableFactoryWrapper.getJoinableFactory().computeJoinCacheKey(clause.getDataSource(), clause.getCondition());
      if (!bytes.isPresent()) {
        // Encountered a data source which didn't support cache yet
        log.debug("skipping caching for join since [%s] does not support caching", clause.getDataSource());
        return new byte[]{};
      }
      keyBuilder.appendByteArray(bytes.get());
      keyBuilder.appendString(clause.getCondition().getOriginalExpression());
      keyBuilder.appendString(clause.getPrefix());
      keyBuilder.appendString(clause.getJoinType().name());
    }
    return keyBuilder.build();
  }
}
