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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.InterpretableRel;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.schema.DatasourceSegmentStats;
import org.apache.druid.sql.calcite.schema.SegmentsRollup;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A leaf {@link BindableRel} that answers the console's {@code GROUP BY datasource} aggregate over
 * {@code sys.segments} from the precomputed {@link SegmentsRollup}, instead of scanning every segment.
 * Produced by {@code SegmentsRollupRule}. Each row is {@code [datasource, agg0, agg1, ...]}: the group
 * key followed by one value per aggregate call, in the order Calcite lays out the {@code Aggregate}
 * output. Per-datasource read authorization is applied here so results match a scan of
 * {@code sys.segments}.
 */
public class SegmentsRollupRel extends AbstractRelNode implements BindableRel
{
  private final RelDataType rowType;
  private final SegmentsRollup rollup;
  private final AuthorizerMapper authorizerMapper;
  /**
   * The querying user, captured from the per-query {@link PlannerContext} at planning time. The
   * interpreter's runtime {@link DataContext} does not carry the authentication result the way the
   * ProjectableFilterableTable scan path does, so we authorize against this instead.
   */
  private final PlannerContext plannerContext;
  /** One extractor per aggregate call, applied in output order after the datasource group key. */
  private final List<Function<DatasourceSegmentStats, Object>> aggExtractors;

  public SegmentsRollupRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelDataType rowType,
      final SegmentsRollup rollup,
      final AuthorizerMapper authorizerMapper,
      final PlannerContext plannerContext,
      final List<Function<DatasourceSegmentStats, Object>> aggExtractors
  )
  {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.rollup = rollup;
    this.authorizerMapper = authorizerMapper;
    this.plannerContext = plannerContext;
    this.aggExtractors = ImmutableList.copyOf(aggExtractors);
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return rowType;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new SegmentsRollupRel(getCluster(), traitSet, rowType, rollup, authorizerMapper, plannerContext, aggExtractors);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    // Reading a per-datasource map is trivial compared with scanning every segment; make it clearly cheap.
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public double estimateRowCount(final RelMetadataQuery mq)
  {
    final ImmutableSortedMap<String, DatasourceSegmentStats> stats = rollup.getStatsIfReady();
    return stats == null ? 1_000d : Math.max(1d, stats.size());
  }

  @Override
  public Class<Object[]> getElementType()
  {
    return Object[].class;
  }

  @Override
  public Enumerable<Object[]> bind(final DataContext dataContext)
  {
    // Reached only if this rel is the top of the tree; otherwise the parent's Interpreter drives
    // implement() below. Either way execution flows through the interpreter Node.
    return new Interpreter(dataContext, this);
  }

  @Override
  public Node implement(final InterpretableRel.InterpreterImplementor implementor)
  {
    final Sink sink = implementor.compiler.sink(this);
    return () -> run(sink);
  }

  private void run(final Sink sink) throws InterruptedException
  {
    final ImmutableSortedMap<String, DatasourceSegmentStats> stats = rollup.getStatsIfReady();
    if (stats != null) {
      rollup.emitAcceleratedQuery();
      final AuthenticationResult authenticationResult = plannerContext.getAuthenticationResult();
      final com.google.common.base.Function<String, Iterable<ResourceAction>> raGenerator =
          datasource -> Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasource));

      for (final String datasource : AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          stats.keySet(),
          raGenerator,
          authorizerMapper
      )) {
        final DatasourceSegmentStats stat = stats.get(datasource);
        final Object[] row = new Object[1 + aggExtractors.size()];
        row[0] = datasource;
        for (int i = 0; i < aggExtractors.size(); i++) {
          row[i + 1] = aggExtractors.get(i).apply(stat);
        }
        sink.send(Row.asCopy(row));
      }
    }
    sink.end();
  }
}
