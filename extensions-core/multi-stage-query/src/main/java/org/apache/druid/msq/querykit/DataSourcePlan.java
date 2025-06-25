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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.lookup.LookupInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.common.SortMergeJoinStageProcessor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.query.planning.JoinDataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Plan for getting data from a {@link DataSource}. Used by {@link QueryKit} implementations.
 */
public class DataSourcePlan
{
  /**
   * A map with {@link DruidSqlInsert#SQL_INSERT_SEGMENT_GRANULARITY} set to null, so we can clear it from the context
   * of subqueries.
   */
  private static final Map<String, Object> CONTEXT_MAP_NO_SEGMENT_GRANULARITY = new HashMap<>();
  private static final Logger log = new Logger(DataSourcePlan.class);

  static {
    CONTEXT_MAP_NO_SEGMENT_GRANULARITY.put(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, null);
  }

  private final DataSource newDataSource;
  private final List<InputSpec> inputSpecs;
  private final IntSet broadcastInputs;

  @Nullable
  private final QueryDefinitionBuilder subQueryDefBuilder;

  DataSourcePlan(
      final DataSource newDataSource,
      final List<InputSpec> inputSpecs,
      final IntSet broadcastInputs,
      @Nullable final QueryDefinitionBuilder subQueryDefBuilder
  )
  {
    this.newDataSource = Preconditions.checkNotNull(newDataSource, "newDataSource");
    this.inputSpecs = Preconditions.checkNotNull(inputSpecs, "inputSpecs");
    this.broadcastInputs = Preconditions.checkNotNull(broadcastInputs, "broadcastInputs");
    this.subQueryDefBuilder = subQueryDefBuilder;

    for (int broadcastInput : broadcastInputs) {
      if (broadcastInput < 0 || broadcastInput >= inputSpecs.size()) {
        throw new IAE("Broadcast input number [%d] out of range [0, %d)", broadcastInput, inputSpecs.size());
      }
    }
  }

  /**
   * Build a plan.
   *
   * @param queryKitSpec     reference for recursive planning
   * @param queryContext     query context
   * @param dataSource       datasource to plan
   * @param querySegmentSpec intervals for mandatory pruning. Must be {@link MultipleIntervalSegmentSpec}. The returned
   *                         plan is guaranteed to be filtered to this interval.
   * @param filter           filter for best-effort pruning. The returned plan may or may not be filtered to this
   *                         filter. Query processing must still apply the filter to generated correct results.
   * @param filterFields     which fields from the filter to consider for pruning, or null to consider all fields.
   * @param minStageNumber   starting stage number for subqueries
   * @param broadcast        whether the plan should broadcast data for this datasource
   */
  public static DataSourcePlan forDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      @Nullable DimFilter filter,
      @Nullable Set<String> filterFields,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    if (!queryContext.isSecondaryPartitionPruningEnabled()) {
      // Clear filter, we don't want to prune today.
      filter = null;
      filterFields = null;
    }

    if (filter != null && filterFields == null) {
      // Ensure filterFields is nonnull if filter is nonnull. Helps for other forXYZ methods, so they don't need to
      // deal with the case where filter is nonnull but filterFields is null.
      filterFields = filter.getRequiredColumns();
    }

    if (dataSource instanceof TableDataSource) {
      return forTable(
          (TableDataSource) dataSource,
          querySegmentSpecIntervals(querySegmentSpec),
          filter,
          filterFields,
          broadcast
      );
    } else if (dataSource instanceof RestrictedDataSource) {
      return forRestricted(
          (RestrictedDataSource) dataSource,
          querySegmentSpecIntervals(querySegmentSpec),
          filter,
          filterFields,
          broadcast
      );
    } else if (dataSource instanceof ExternalDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forExternal((ExternalDataSource) dataSource, broadcast);
    } else if (dataSource instanceof InlineDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forInline((InlineDataSource) dataSource, broadcast);
    } else if (dataSource instanceof LookupDataSource) {
      return forLookup((LookupDataSource) dataSource, broadcast);
    } else if (dataSource instanceof FilteredDataSource) {
      return forFilteredDataSource(
          queryKitSpec,
          queryContext,
          (FilteredDataSource) dataSource,
          querySegmentSpec,
          minStageNumber,
          broadcast
      );
    } else if (dataSource instanceof UnnestDataSource) {
      return forUnnest(
          queryKitSpec,
          queryContext,
          (UnnestDataSource) dataSource,
          querySegmentSpec,
          minStageNumber,
          broadcast
      );
    } else if (dataSource instanceof QueryDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forQuery(
          queryKitSpec,
          (QueryDataSource) dataSource,
          minStageNumber,
          broadcast
      );
    } else if (dataSource instanceof UnionDataSource) {
      return forUnion(
          queryKitSpec,
          queryContext,
          (UnionDataSource) dataSource,
          querySegmentSpec,
          filter,
          filterFields,
          minStageNumber,
          broadcast
      );
    } else if (dataSource instanceof JoinDataSource) {
      JoinDataSource joinDataSource = (JoinDataSource) dataSource;
      final JoinAlgorithm preferredJoinAlgorithm = joinDataSource.getJoinAlgorithm();
      final JoinAlgorithm deducedJoinAlgorithm = deduceJoinAlgorithm(
          preferredJoinAlgorithm,
          joinDataSource
      );

      switch (deducedJoinAlgorithm) {
        case BROADCAST:
          return forBroadcastHashJoin(
              queryKitSpec,
              queryContext,
              joinDataSource,
              querySegmentSpec,
              filter,
              filterFields,
              minStageNumber,
              broadcast
          );

        case SORT_MERGE:
          return forSortMergeJoin(
              queryKitSpec,
              joinDataSource,
              querySegmentSpec,
              minStageNumber,
              broadcast
          );

        default:
          throw new UOE("Cannot handle join algorithm [%s]", deducedJoinAlgorithm);
      }
    } else {
      throw new UOE("Cannot handle dataSource [%s]", dataSource);
    }
  }

  /**
   * Possibly remapped datasource that should be used when processing. Will be either the original datasource, or the
   * original datasource with itself or some children replaced by {@link InputNumberDataSource}. Any added
   * {@link InputNumberDataSource} refer to {@link StageInputSpec} in {@link #getInputSpecs()}.
   */
  public DataSource getNewDataSource()
  {
    return newDataSource;
  }

  /**
   * Input specs that should be used when processing.
   */
  public List<InputSpec> getInputSpecs()
  {
    return inputSpecs;
  }

  /**
   * Which input specs from {@link #getInputSpecs()} are broadcast.
   */
  public IntSet getBroadcastInputs()
  {
    return broadcastInputs;
  }

  /**
   * Figure for {@link StageDefinition#getMaxWorkerCount()} that should be used when processing.
   */
  public int getMaxWorkerCount(final QueryKitSpec queryKitSpec)
  {
    if (isSingleWorker()) {
      return 1;
    } else if (InputSpecs.hasLeafInputs(inputSpecs, broadcastInputs)) {
      return queryKitSpec.getMaxLeafWorkerCount();
    } else {
      return queryKitSpec.getMaxNonLeafWorkerCount();
    }
  }

  /**
   * Returns a {@link QueryDefinitionBuilder} that includes any {@link StageInputSpec} from {@link #getInputSpecs()}.
   * Absent if this plan does not involve reading from prior stages.
   */
  public Optional<QueryDefinitionBuilder> getSubQueryDefBuilder()
  {
    return Optional.ofNullable(subQueryDefBuilder);
  }

  /**
   * Contains the logic that deduces the join algorithm to be used. Ideally, this should reside while planning the
   * native query, however we don't have the resources and the structure in place (when adding this function) to do so.
   * Therefore, this is done while planning the MSQ query
   * It takes into account the algorithm specified by "sqlJoinAlgorithm" in the query context and the join condition
   * that is present in the query.
   */
  private static JoinAlgorithm deduceJoinAlgorithm(JoinAlgorithm preferredJoinAlgorithm, JoinDataSource joinDataSource)
  {
    JoinAlgorithm deducedJoinAlgorithm;
    if (JoinAlgorithm.BROADCAST.equals(preferredJoinAlgorithm)) {
      deducedJoinAlgorithm = JoinAlgorithm.BROADCAST;
    } else if (canUseSortMergeJoin(joinDataSource.getConditionAnalysis())) {
      deducedJoinAlgorithm = JoinAlgorithm.SORT_MERGE;
    } else {
      deducedJoinAlgorithm = JoinAlgorithm.BROADCAST;
    }

    if (deducedJoinAlgorithm != preferredJoinAlgorithm) {
      log.debug(
          "User wanted to plan join [%s] as [%s], however the join will be executed as [%s]",
          joinDataSource,
          preferredJoinAlgorithm.toString(),
          deducedJoinAlgorithm.toString()
      );
    }

    return deducedJoinAlgorithm;
  }

  /**
   * Checks if the sortMerge algorithm can execute a particular join condition.
   * <p>
   * One check: join condition on two tables "table1" and "table2" is of the form
   * table1.columnA = table2.columnA && table1.columnB = table2.columnB && ....
   */
  private static boolean canUseSortMergeJoin(JoinConditionAnalysis joinConditionAnalysis)
  {
    return joinConditionAnalysis
        .getEquiConditions()
        .stream()
        .allMatch(equality -> equality.getLeftExpr().isIdentifier());
  }

  /**
   * Whether this datasource must be processed by a single worker. True if, and only if, all inputs are broadcast.
   */
  public boolean isSingleWorker()
  {
    return broadcastInputs.size() == inputSpecs.size();
  }

  private static DataSourcePlan forTable(
      final TableDataSource dataSource,
      final List<Interval> intervals,
      @Nullable final DimFilter filter,
      @Nullable final Set<String> filterFields,
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        (broadcast && dataSource.isGlobal()) ? dataSource : new InputNumberDataSource(0),
        Collections.singletonList(new TableInputSpec(dataSource.getName(), intervals, filter, filterFields)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  private static DataSourcePlan forRestricted(
      final RestrictedDataSource dataSource,
      final List<Interval> intervals,
      @Nullable final DimFilter filter,
      @Nullable final Set<String> filterFields,
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        (broadcast && dataSource.isGlobal())
        ? dataSource
        : new RestrictedInputNumberDataSource(0, dataSource.getPolicy()),
        Collections.singletonList(new TableInputSpec(dataSource.getBase().getName(), intervals, filter, filterFields)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  private static DataSourcePlan forExternal(
      final ExternalDataSource dataSource,
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        dataSource,
        Collections.singletonList(
            new ExternalInputSpec(
                dataSource.getInputSource(),
                dataSource.getInputFormat(),
                dataSource.getSignature()
            )
        ),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  private static DataSourcePlan forInline(
      final InlineDataSource dataSource,
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        dataSource,
        Collections.singletonList(new InlineInputSpec(dataSource)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  private static DataSourcePlan forLookup(
      final LookupDataSource dataSource,
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        dataSource,
        Collections.singletonList(new LookupInputSpec(dataSource.getLookupName())),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }

  private static DataSourcePlan forQuery(
      final QueryKitSpec queryKitSpec,
      final QueryDataSource dataSource,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final QueryDefinition subQueryDef = queryKitSpec.getQueryKit().makeQueryDefinition(
        queryKitSpec,
        // Subqueries ignore SQL_INSERT_SEGMENT_GRANULARITY, even if set in the context. It's only used for the
        // outermost query, and setting it for the subquery makes us erroneously add bucketing where it doesn't belong.
        dataSource.getQuery().withOverriddenContext(CONTEXT_MAP_NO_SEGMENT_GRANULARITY),
        ShuffleSpecFactories.globalSortWithMaxPartitionCount(queryKitSpec.getNumPartitionsForShuffle()),
        minStageNumber
    );

    final int stageNumber = subQueryDef.getFinalStageDefinition().getStageNumber();

    return new DataSourcePlan(
        new InputNumberDataSource(0),
        Collections.singletonList(new StageInputSpec(stageNumber)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        QueryDefinition.builder(subQueryDef)
    );
  }

  private static DataSourcePlan forFilteredDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final FilteredDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final DataSourcePlan basePlan = forDataSource(
        queryKitSpec,
        queryContext,
        dataSource.getBase(),
        querySegmentSpec,
        null,
        null,
        minStageNumber,
        broadcast
    );

    DataSource newDataSource = basePlan.getNewDataSource();

    final List<InputSpec> inputSpecs = new ArrayList<>(basePlan.getInputSpecs());
    newDataSource = FilteredDataSource.create(newDataSource, dataSource.getFilter());
    return new DataSourcePlan(
        newDataSource,
        inputSpecs,
        basePlan.getBroadcastInputs(),
        basePlan.getSubQueryDefBuilder().orElse(null)
    );

  }

  /**
   * Build a plan for Unnest data source
   */
  private static DataSourcePlan forUnnest(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final UnnestDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    // Find the plan for base data source by recursing
    final DataSourcePlan basePlan = forDataSource(
        queryKitSpec,
        queryContext,
        dataSource.getBase(),
        querySegmentSpec,
        null,
        null,
        minStageNumber,
        broadcast
    );
    DataSource newDataSource = basePlan.getNewDataSource();

    final List<InputSpec> inputSpecs = new ArrayList<>(basePlan.getInputSpecs());

    // Create the new data source using the data source from the base plan
    newDataSource = UnnestDataSource.create(
        newDataSource,
        dataSource.getVirtualColumn(),
        dataSource.getUnnestFilter()
    );
    // The base data source can be a join and might already have broadcast inputs
    // Need to set the broadcast inputs from the basePlan
    return new DataSourcePlan(
        newDataSource,
        inputSpecs,
        basePlan.getBroadcastInputs(),
        basePlan.getSubQueryDefBuilder().orElse(null)
    );
  }

  private static DataSourcePlan forUnion(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final UnionDataSource unionDataSource,
      final QuerySegmentSpec querySegmentSpec,
      @Nullable DimFilter filter,
      @Nullable Set<String> filterFields,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    // This is done to prevent loss of generality since MSQ can plan any type of DataSource.
    List<DataSource> children = unionDataSource.getChildren();

    final QueryDefinitionBuilder subqueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());
    final List<DataSource> newChildren = new ArrayList<>();
    final List<InputSpec> inputSpecs = new ArrayList<>();
    final IntSet broadcastInputs = new IntOpenHashSet();

    for (DataSource child : children) {
      DataSourcePlan childDataSourcePlan = forDataSource(
          queryKitSpec,
          queryContext,
          child,
          querySegmentSpec,
          filter,
          filterFields,
          Math.max(minStageNumber, subqueryDefBuilder.getNextStageNumber()),
          broadcast
      );

      int shift = inputSpecs.size();

      newChildren.add(shiftInputNumbers(childDataSourcePlan.getNewDataSource(), shift));
      inputSpecs.addAll(childDataSourcePlan.getInputSpecs());
      childDataSourcePlan.getSubQueryDefBuilder().ifPresent(subqueryDefBuilder::addAll);
      childDataSourcePlan.getBroadcastInputs().forEach(inp -> broadcastInputs.add(inp + shift));
    }
    return new DataSourcePlan(
        new UnionDataSource(newChildren),
        inputSpecs,
        broadcastInputs,
        subqueryDefBuilder
    );
  }

  /**
   * Build a plan for broadcast hash-join.
   */
  private static DataSourcePlan forBroadcastHashJoin(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      @Nullable final DimFilter filter,
      @Nullable final Set<String> filterFields,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());
    final JoinDataSourceAnalysis analysis = dataSource.getJoinAnalysisForDataSource();

    final DataSourcePlan basePlan = forDataSource(
        queryKitSpec,
        queryContext,
        analysis.getBaseDataSource(),
        querySegmentSpec,
        filter,
        filter == null ? null : DimFilterUtils.onlyBaseFields(filterFields, analysis::isBaseColumn),
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        broadcast
    );

    DataSource newDataSource = basePlan.getNewDataSource();
    final List<InputSpec> inputSpecs = new ArrayList<>(basePlan.getInputSpecs());
    final IntSet broadcastInputs = new IntOpenHashSet(basePlan.getBroadcastInputs());
    basePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    for (int i = 0; i < analysis.getPreJoinableClauses().size(); i++) {
      final PreJoinableClause clause = analysis.getPreJoinableClauses().get(i);
      final DataSourcePlan clausePlan = forDataSource(
          queryKitSpec,
          queryContext,
          clause.getDataSource(),
          new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
          null, // Don't push down query filters for right-hand side: needs some work to ensure it works properly.
          null,
          Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
          true // Always broadcast right-hand side of the join.
      );

      // Shift all input numbers in the clausePlan.
      final int shift = inputSpecs.size();

      newDataSource = JoinDataSource.create(
          newDataSource,
          shiftInputNumbers(clausePlan.getNewDataSource(), shift),
          clause.getPrefix(),
          clause.getCondition(),
          clause.getJoinType(),
          // First JoinDataSource (i == 0) involves the base table, so we need to propagate the base table filter.
          i == 0 ? analysis.getJoinBaseTableFilter().orElse(null) : null,
          dataSource.getJoinableFactoryWrapper(),
          clause.getJoinAlgorithm()
      );
      inputSpecs.addAll(clausePlan.getInputSpecs());
      clausePlan.getBroadcastInputs().intStream().forEach(n -> broadcastInputs.add(n + shift));
      clausePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);
    }

    return new DataSourcePlan(newDataSource, inputSpecs, broadcastInputs, subQueryDefBuilder);
  }

  /**
   * Build a plan for sort-merge join.
   */
  private static DataSourcePlan forSortMergeJoin(
      final QueryKitSpec queryKitSpec,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
    SortMergeJoinStageProcessor.validateCondition(dataSource.getConditionAnalysis());

    // Partition by keys given by the join condition.
    final List<List<KeyColumn>> partitionKeys = SortMergeJoinStageProcessor.toKeyColumns(
        SortMergeJoinStageProcessor.validateCondition(dataSource.getConditionAnalysis())
    );

    final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());

    // Plan the left input.
    // We're confident that we can cast dataSource.getLeft() to QueryDataSource, because DruidJoinQueryRel creates
    // subqueries when the join algorithm is sortMerge.
    final DataSourcePlan leftPlan = forQuery(
        queryKitSpec,
        (QueryDataSource) dataSource.getLeft(),
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        false
    );
    leftPlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    // Plan the right input.
    // We're confident that we can cast dataSource.getRight() to QueryDataSource, because DruidJoinQueryRel creates
    // subqueries when the join algorithm is sortMerge.
    final DataSourcePlan rightPlan = forQuery(
        queryKitSpec,
        (QueryDataSource) dataSource.getRight(),
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        false
    );
    rightPlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    // Build up the left stage.
    final StageDefinitionBuilder leftBuilder = subQueryDefBuilder.getStageBuilder(
        ((StageInputSpec) Iterables.getOnlyElement(leftPlan.getInputSpecs())).getStageNumber()
    );

    final int hashPartitionCount = queryKitSpec.getNumPartitionsForShuffle();
    final List<KeyColumn> leftPartitionKey = partitionKeys.get(0);
    leftBuilder.shuffleSpec(new HashShuffleSpec(new ClusterBy(leftPartitionKey, 0), hashPartitionCount));
    leftBuilder.signature(QueryKitUtils.sortableSignature(leftBuilder.getSignature(), leftPartitionKey));

    // Build up the right stage.
    final StageDefinitionBuilder rightBuilder = subQueryDefBuilder.getStageBuilder(
        ((StageInputSpec) Iterables.getOnlyElement(rightPlan.getInputSpecs())).getStageNumber()
    );

    final List<KeyColumn> rightPartitionKey = partitionKeys.get(1);
    rightBuilder.shuffleSpec(new HashShuffleSpec(new ClusterBy(rightPartitionKey, 0), hashPartitionCount));
    rightBuilder.signature(QueryKitUtils.sortableSignature(rightBuilder.getSignature(), rightPartitionKey));

    // Compute join signature.
    final RowSignature.Builder joinSignatureBuilder = RowSignature.builder();

    for (String leftColumn : leftBuilder.getSignature().getColumnNames()) {
      joinSignatureBuilder.add(leftColumn, leftBuilder.getSignature().getColumnType(leftColumn).orElse(null));
    }

    for (String rightColumn : rightBuilder.getSignature().getColumnNames()) {
      joinSignatureBuilder.add(
          dataSource.getRightPrefix() + rightColumn,
          rightBuilder.getSignature().getColumnType(rightColumn).orElse(null)
      );
    }

    // Build up the join stage.
    final int stageNumber = Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber());

    subQueryDefBuilder.add(
        StageDefinition.builder(stageNumber)
                       .inputs(
                           ImmutableList.of(
                               Iterables.getOnlyElement(leftPlan.getInputSpecs()),
                               Iterables.getOnlyElement(rightPlan.getInputSpecs())
                           )
                       )
                       .maxWorkerCount(queryKitSpec.getMaxNonLeafWorkerCount())
                       .signature(joinSignatureBuilder.build())
                       .processor(
                           new SortMergeJoinStageProcessor(
                               dataSource.getRightPrefix(),
                               dataSource.getConditionAnalysis(),
                               dataSource.getJoinType()
                           )
                       )
    );

    return new DataSourcePlan(
        new InputNumberDataSource(0),
        Collections.singletonList(new StageInputSpec(stageNumber)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        subQueryDefBuilder
    );
  }

  private static DataSource shiftInputNumbers(final DataSource dataSource, final int shift)
  {
    if (shift < 0) {
      throw new IAE("Shift must be >= 0");
    } else if (shift == 0) {
      return dataSource;
    } else {
      if (dataSource instanceof InputNumberDataSource) {
        return new InputNumberDataSource(((InputNumberDataSource) dataSource).getInputNumber() + shift);
      } else {
        return dataSource.withChildren(
            dataSource.getChildren()
                      .stream()
                      .map(child -> shiftInputNumbers(child, shift))
                      .collect(Collectors.toList())
        );
      }
    }
  }

  private static List<Interval> querySegmentSpecIntervals(final QuerySegmentSpec querySegmentSpec)
  {
    if (querySegmentSpec instanceof MultipleIntervalSegmentSpec) {
      return querySegmentSpec.getIntervals();
    } else {
      throw new UOE("Cannot handle querySegmentSpec type [%s]", querySegmentSpec.getClass().getName());
    }
  }

  /**
   * Verify that the provided {@link QuerySegmentSpec} is a {@link MultipleIntervalSegmentSpec} with
   * interval {@link Intervals#ETERNITY}. If not, throw an {@link UnsupportedOperationException}.
   *
   * We don't need to support this for anything that is not {@link DataSourceAnalysis#isTableBased()}, because
   * the SQL layer avoids "intervals" in other cases. See
   * {@link org.apache.druid.sql.calcite.rel.DruidQuery#canUseIntervalFiltering(DataSource)}.
   */
  private static void checkQuerySegmentSpecIsEternity(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec
  )
  {
    final boolean querySegmentSpecIsEternity =
        querySegmentSpec instanceof MultipleIntervalSegmentSpec
        && querySegmentSpec.getIntervals().equals(Intervals.ONLY_ETERNITY);

    if (!querySegmentSpecIsEternity) {
      throw new UOE(
          "Cannot filter datasource [%s] using [%s]",
          dataSource.getClass().getName(),
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }
}
