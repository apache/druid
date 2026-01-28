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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.JoinDataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
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
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
 * When analyzed by {@link JoinDataSourceAnalysis}, the right-hand side of this datasource
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
  private final JoinAlgorithm joinAlgorithm;
  private static final Logger log = new Logger(JoinDataSource.class);

  private JoinDataSource(
      DataSource left,
      DataSource right,
      String rightPrefix,
      JoinConditionAnalysis conditionAnalysis,
      JoinType joinType,
      @Nullable DimFilter leftFilter,
      @Nullable JoinableFactoryWrapper joinableFactoryWrapper,
      JoinAlgorithm joinAlgorithm
  )
  {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = JoinPrefixUtils.validatePrefix(rightPrefix);
    this.conditionAnalysis = Preconditions.checkNotNull(conditionAnalysis, "conditionAnalysis");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    this.leftFilter = validateLeftFilter(left, leftFilter);
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.joinAlgorithm = JoinAlgorithm.BROADCAST.equals(joinAlgorithm) ? null : joinAlgorithm;
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
      @Nullable @JacksonInject JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable @JsonProperty("joinAlgorithm") JoinAlgorithm joinAlgorithm
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
        joinableFactoryWrapper,
        joinAlgorithm
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
      @Nullable final JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable final JoinAlgorithm joinAlgorithm
  )
  {
    return new JoinDataSource(
        left,
        right,
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
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
        joinableFactoryWrapper,
        joinAlgorithm
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
  public boolean isProcessable()
  {
    return left.isProcessable() && right.isGlobal();
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
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder keyBuilder;
    keyBuilder = new CacheKeyBuilder(DataSource.JOIN_OPERATION_CACHE_ID);
    keyBuilder.appendCacheable(leftFilter);
    keyBuilder.appendCacheable(conditionAnalysis);
    keyBuilder.appendCacheable(joinType);
    keyBuilder.appendCacheable(left);
    keyBuilder.appendCacheable(right);
    return keyBuilder.build();
  }

  @JsonProperty("joinAlgorithm")
  @JsonInclude(Include.NON_NULL)
  private JoinAlgorithm getJoinAlgorithmForSerialization()
  {
    return joinAlgorithm;
  }

  public JoinAlgorithm getJoinAlgorithm()
  {
    return joinAlgorithm == null ? JoinAlgorithm.BROADCAST : joinAlgorithm;
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
           joinAlgorithm == that.joinAlgorithm &&
           joinType == that.joinType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(left, right, rightPrefix, conditionAnalysis, joinType, leftFilter, joinAlgorithm);
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
           ", joinAlgorithm=" + joinAlgorithm +
           '}';
  }

  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   * @param query
   */
  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    JoinDataSourceAnalysis joinAnalysis = getJoinAnalysisForDataSource();
    List<PreJoinableClause> clauses = joinAnalysis.getPreJoinableClauses();
    Filter baseFilter = joinAnalysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null);

    if (clauses.isEmpty()) {
      throw DruidException.defensive("A JoinDataSource with no join clauses should not be mapped.");
    }
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

      baseFilterToUse = Filters.maybeAnd(
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

    final SegmentMapFunction baseMapFn = joinAnalysis.getBaseDataSource().createSegmentMapFunction(query);

    return createSegmentMapFunction(clausesToUse, baseFilterToUse, joinFilterPreAnalysis, baseMapFn);
  }

  public static SegmentMapFunction createSegmentMapFunction(
      List<JoinableClause> clausesToUse,
      Filter baseFilterToUse,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      SegmentMapFunction baseMapFn
  )
  {
    return baseSegmentReference -> {
      final Optional<Segment> maybeBaseSegment = baseMapFn.apply(baseSegmentReference);
      if (maybeBaseSegment.isPresent()) {
        final Segment baseSegment = maybeBaseSegment.get();
        Closer closer = Closer.create();
        // this could be a bit cleaner if joinables get reference returned a closeable joinable (to be consistent with
        // segment) then we could build a new list of closeable joinables and just close them like we do the base
        // segment in HashJoinSegment
        try {
          boolean acquireFailed = false;

          for (JoinableClause joinClause : clausesToUse) {
            if (acquireFailed) {
              break;
            }
            acquireFailed = joinClause.acquireReference().map(closeable -> {
              closer.register(closeable);
              return false;
            }).orElse(true);
          }
          if (acquireFailed) {
            CloseableUtils.closeAndWrapExceptions(closer);
            CloseableUtils.closeAndWrapExceptions(baseSegment);
            return Optional.empty();
          } else {
            return Optional.of(
                createHashJoinSegment(
                    baseSegment,
                    baseFilterToUse,
                    clausesToUse,
                    joinFilterPreAnalysis,
                    closer
                )
            );
          }
        }
        catch (Throwable e) {
          // acquireReferences is not permitted to throw exceptions.
          CloseableUtils.closeAndSuppressExceptions(closer, e::addSuppressed);
          CloseableUtils.closeAndSuppressExceptions(baseSegment, e::addSuppressed);
          log.warn(e, "Exception encountered while trying to acquire reference");
          return Optional.empty();
        }
      }
      return Optional.empty();
    };
  }

  private static Segment createHashJoinSegment(
      Segment sourceSegment,
      Filter baseFilterToUse,
      List<JoinableClause> clausesToUse,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      Closeable closeable
  )
  {
    if (clausesToUse.isEmpty() && baseFilterToUse == null) {
      return sourceSegment;
    }
    return new HashJoinSegment(sourceSegment, baseFilterToUse, clausesToUse, joinFilterPreAnalysis, closeable);
  }

  /**
   * Computes the DataSourceAnalysis with join boundaries.
   *
   * It will only process what the join datasource could handle in one go - and not more.
   */
  @VisibleForTesting
  public JoinDataSourceAnalysis getJoinAnalysisForDataSource()
  {
    return JoinDataSourceAnalysis.constructAnalysis(this);
  }

  /**
   * Builds the DataSourceAnalysis for this join.
   *
   * @param vertexBoundary if the returned analysis should go up to the vertex boundary.
   *
   * @throws IllegalArgumentException if dataSource cannot be fully flattened.
   */
  private static JoinDataSourceAnalysis constructAnalysis(final JoinDataSource dataSource, boolean vertexBoundary)
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
      if (vertexBoundary) {
        if (current instanceof UnnestDataSource) {
          final UnnestDataSource unnestDataSource = (UnnestDataSource) current;
          current = unnestDataSource.getBase();
          continue;
        } else if (current instanceof FilteredDataSource) {
          final FilteredDataSource filteredDataSource = (FilteredDataSource) current;
          current = filteredDataSource.getBase();
          continue;
        }
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


  /**
   * Validates whether the provided leftFilter is permitted to apply to the provided left-hand datasource. Throws an
   * exception if the combination is invalid. Returns the filter if the combination is valid.
   */
  @Nullable
  private static DimFilter validateLeftFilter(final DataSource leftDataSource, @Nullable final DimFilter leftFilter)
  {
    if (leftFilter == null || TrueDimFilter.instance().equals(leftFilter)) {
      return null;
    }
    // Currently we only support leftFilter when applied to concrete leaf datasources (ones with no children).
    // Note that this mean we don't support unions of table, even though this would be reasonable to add in the future.
    Preconditions.checkArgument(
        leftDataSource.isProcessable() && leftDataSource.getChildren().isEmpty(),
        "left filter is only supported if left data source is direct table access"
    );
    return leftFilter;
  }
}
