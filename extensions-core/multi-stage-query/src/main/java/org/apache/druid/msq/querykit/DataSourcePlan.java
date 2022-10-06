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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.NilInputSource;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Used by {@link QueryKit} implementations to produce {@link InputSpec} from native {@link DataSource}.
 */
public class DataSourcePlan
{
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

  @SuppressWarnings("rawtypes")
  public static DataSourcePlan forDataSource(
      final QueryKit queryKit,
      final String queryId,
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      @Nullable DimFilter filter,
      final int maxWorkerCount,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    if (dataSource instanceof TableDataSource) {
      return forTable((TableDataSource) dataSource, querySegmentSpecIntervals(querySegmentSpec), filter, broadcast);
    } else if (dataSource instanceof ExternalDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forExternal((ExternalDataSource) dataSource, broadcast);
    } else if (dataSource instanceof InlineDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forInline((InlineDataSource) dataSource, broadcast);
    } else if (dataSource instanceof QueryDataSource) {
      checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
      return forQuery(
          queryKit,
          queryId,
          (QueryDataSource) dataSource,
          maxWorkerCount,
          minStageNumber,
          broadcast
      );
    } else if (dataSource instanceof JoinDataSource) {
      return forJoin(
          queryKit,
          queryId,
          (JoinDataSource) dataSource,
          querySegmentSpec,
          maxWorkerCount,
          minStageNumber,
          broadcast
      );
    } else {
      throw new UOE("Cannot handle dataSource [%s]", dataSource);
    }
  }

  public DataSource getNewDataSource()
  {
    return newDataSource;
  }

  public List<InputSpec> getInputSpecs()
  {
    return inputSpecs;
  }

  public IntSet getBroadcastInputs()
  {
    return broadcastInputs;
  }

  public Optional<QueryDefinitionBuilder> getSubQueryDefBuilder()
  {
    return Optional.ofNullable(subQueryDefBuilder);
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
      final boolean broadcast
  )
  {
    return new DataSourcePlan(
        new InputNumberDataSource(0),
        Collections.singletonList(new TableInputSpec(dataSource.getName(), intervals, filter)),
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
        new InputNumberDataSource(0),
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
    final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
    final RowSignature signature = dataSource.getRowSignature();
    final StringBuilder stringBuilder = new StringBuilder();

    for (final Object[] rowArray : dataSource.getRows()) {
      final Map<String, Object> m = new HashMap<>();

      for (int i = 0; i < signature.size(); i++) {
        m.put(signature.getColumnName(i), rowArray[i]);
      }

      try {
        stringBuilder.append(jsonMapper.writeValueAsString(m)).append('\n');
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    final String dataString = stringBuilder.toString();

    return forExternal(
        new ExternalDataSource(
            dataString.isEmpty() ? NilInputSource.instance() : new InlineInputSource(dataString),
            new JsonInputFormat(null, null, null, null, null),
            signature
        ),
        broadcast
    );
  }

  private static DataSourcePlan forQuery(
      final QueryKit queryKit,
      final String queryId,
      final QueryDataSource dataSource,
      final int maxWorkerCount,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final QueryDefinition subQueryDef = queryKit.makeQueryDefinition(
        queryId,
        dataSource.getQuery(),
        queryKit,
        ShuffleSpecFactories.subQueryWithMaxWorkerCount(maxWorkerCount),
        maxWorkerCount,
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

  private static DataSourcePlan forJoin(
      final QueryKit queryKit,
      final String queryId,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int maxWorkerCount,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder();
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(dataSource);

    final DataSourcePlan basePlan = forDataSource(
        queryKit,
        queryId,
        analysis.getBaseDataSource(),
        querySegmentSpec,
        null, // Don't push query filters down through a join: this needs some work to ensure pruning works properly.
        maxWorkerCount,
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
          queryKit,
          queryId,
          clause.getDataSource(),
          new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
          null, // Don't push query filters down through a join: this needs some work to ensure pruning works properly.
          maxWorkerCount,
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
          i == 0 ? analysis.getJoinBaseTableFilter().orElse(null) : null
      );
      inputSpecs.addAll(clausePlan.getInputSpecs());
      clausePlan.getBroadcastInputs().intStream().forEach(n -> broadcastInputs.add(n + shift));
      clausePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);
    }

    return new DataSourcePlan(newDataSource, inputSpecs, broadcastInputs, subQueryDefBuilder);
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
   * Anywhere this appears is a place that we do not support using the "intervals" parameter of a query
   * (i.e., {@link org.apache.druid.query.BaseQuery#getQuerySegmentSpec()}) for time filtering. Ideally,
   * we'd support this everywhere it appears, but we can get away without it for now.
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
