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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Builder for a Druid query, not counting the "dataSource" (which will be slotted in later).
 */
public class PartialDruidQuery
{
  private final Supplier<RelBuilder> builderSupplier;
  private final RelNode scan;
  private final Filter whereFilter;
  private final Project selectProject;
  private final Aggregate aggregate;
  private final Filter havingFilter;
  private final Project aggregateProject;
  private final Sort sort;
  private final Project sortProject;
  private final Window window;
  private final Project windowProject;

  public enum Stage
  {
    // SCAN must be present on all queries.
    SCAN,

    // WHERE_FILTER, SELECT_PROJECT may be present on any query, except ones with WINDOW.
    WHERE_FILTER,
    SELECT_PROJECT {
      @Override
      public boolean canFollow(Stage stage)
      {
        // SELECT_PROJECT can be stacked on top of another SELECT_PROJECT.
        return stage.compareTo(this) <= 0;
      }
    },

    // AGGREGATE, HAVING_FILTER, AGGREGATE_PROJECT can be present on non-WINDOW aggregating queries.
    AGGREGATE,
    HAVING_FILTER {
      @Override
      public boolean canFollow(Stage stage)
      {
        return stage == AGGREGATE;
      }
    },
    AGGREGATE_PROJECT {
      @Override
      public boolean canFollow(Stage stage)
      {
        return stage == AGGREGATE || stage == HAVING_FILTER;
      }
    },

    // SORT, SORT_PROJECT may be present on any query, except ones with WINDOW.
    SORT,
    SORT_PROJECT {
      @Override
      public boolean canFollow(Stage stage)
      {
        return stage == SORT;
      }
    },

    // WINDOW, WINDOW_PROJECT may be present only together with SCAN.
    WINDOW {
      @Override
      public boolean canFollow(Stage stage)
      {
        return stage == SCAN;
      }
    },
    WINDOW_PROJECT {
      @Override
      public boolean canFollow(Stage stage)
      {
        return stage == WINDOW;
      }
    };

    public boolean canFollow(final Stage stage)
    {
      return stage.compareTo(this) < 0;
    }
  }

  private PartialDruidQuery(
      final Supplier<RelBuilder> builderSupplier,
      final RelNode scan,
      final Filter whereFilter,
      final Project selectProject,
      final Aggregate aggregate,
      final Project aggregateProject,
      final Filter havingFilter,
      final Sort sort,
      final Project sortProject,
      final Window window,
      final Project windowProject
  )
  {
    this.builderSupplier = Preconditions.checkNotNull(builderSupplier, "builderSupplier");
    this.scan = Preconditions.checkNotNull(scan, "scan");
    this.whereFilter = whereFilter;
    this.selectProject = selectProject;
    this.aggregate = aggregate;
    this.aggregateProject = aggregateProject;
    this.havingFilter = havingFilter;
    this.sort = sort;
    this.sortProject = sortProject;
    this.window = window;
    this.windowProject = windowProject;
  }

  public static PartialDruidQuery create(final RelNode inputRel)
  {
    final Supplier<RelBuilder> builderSupplier = () -> RelFactories.LOGICAL_BUILDER.create(
        inputRel.getCluster(),
        inputRel.getTable() != null ? inputRel.getTable().getRelOptSchema() : null
    );
    return new PartialDruidQuery(builderSupplier, inputRel, null, null, null, null, null, null, null, null, null);
  }

  public static PartialDruidQuery createOuterQuery(final PartialDruidQuery inputQuery, PlannerContext plannerContext)
  {
    final RelNode inputRel = inputQuery.leafRel();
    return create(
        inputRel.copy(
            inputQuery.getTraitSet(inputRel.getConvention(), plannerContext),
            inputRel.getInputs()
        )
    );
  }

  public RelNode getScan()
  {
    return scan;
  }

  public Filter getWhereFilter()
  {
    return whereFilter;
  }

  public Project getSelectProject()
  {
    return selectProject;
  }

  public Aggregate getAggregate()
  {
    return aggregate;
  }

  public Filter getHavingFilter()
  {
    return havingFilter;
  }

  public Project getAggregateProject()
  {
    return aggregateProject;
  }

  public Sort getSort()
  {
    return sort;
  }

  public Project getSortProject()
  {
    return sortProject;
  }

  public Window getWindow()
  {
    return window;
  }

  public Project getWindowProject()
  {
    return windowProject;
  }

  public PartialDruidQuery withWhereFilter(final Filter newWhereFilter)
  {
    validateStage(Stage.WHERE_FILTER);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        newWhereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withSelectProject(final Project newSelectProject)
  {
    validateStage(Stage.SELECT_PROJECT);

    // Possibly merge together two projections.
    final Project theProject;
    if (selectProject == null) {
      theProject = newSelectProject;
    } else {
      return mergeProject(newSelectProject);
    }

    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        theProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery mergeProject(Project newSelectProject)
  {
    if (stage() != Stage.SELECT_PROJECT) {
      throw new ISE("Expected partial query state to be [%s], but found [%s]", Stage.SELECT_PROJECT, stage());
    }
    Project theProject;
    final List<RexNode> newProjectRexNodes = RelOptUtil.pushPastProject(
        newSelectProject.getProjects(),
        selectProject
    );

    if (RexUtil.isIdentity(newProjectRexNodes, selectProject.getInput().getRowType())) {
      // The projection is gone.
      theProject = null;
    } else {
      final RelBuilder relBuilder = builderSupplier.get();
      relBuilder.push(selectProject.getInput());
      relBuilder.project(
          newProjectRexNodes,
          newSelectProject.getRowType().getFieldNames(),
          true
      );
      theProject = (Project) relBuilder.build();
    }
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        theProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withAggregate(final Aggregate newAggregate)
  {
    validateStage(Stage.AGGREGATE);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        newAggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withHavingFilter(final Filter newHavingFilter)
  {
    validateStage(Stage.HAVING_FILTER);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        newHavingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withAggregateProject(final Project newAggregateProject)
  {
    validateStage(Stage.AGGREGATE_PROJECT);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        newAggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withSort(final Sort newSort)
  {
    validateStage(Stage.SORT);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        havingFilter,
        newSort,
        sortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withSortProject(final Project newSortProject)
  {
    validateStage(Stage.SORT_PROJECT);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        newSortProject,
        window,
        windowProject
    );
  }

  public PartialDruidQuery withWindow(final Window newWindow)
  {
    validateStage(Stage.WINDOW);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        newWindow,
        windowProject
    );
  }

  public PartialDruidQuery withWindowProject(final Project newWindowProject)
  {
    validateStage(Stage.WINDOW_PROJECT);
    return new PartialDruidQuery(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject,
        window,
        newWindowProject
    );
  }

  public RelDataType getRowType()
  {
    return leafRel().getRowType();
  }

  /**
   * Get traits for this partial query.
   *
   * This is the traits from {@link #leafRel()}, plus {@link RelCollationTraitDef} if {@link #stage()} is
   * {@link Stage#AGGREGATE} or {@link Stage#AGGREGATE_PROJECT} (to represent the fact that Druid sorts by grouping
   * keys when grouping).
   *
   * @param convention convention to include in the returned array
   */
  public RelTraitSet getTraitSet(final Convention convention, final PlannerContext plannerContext)
  {
    final RelTraitSet leafRelTraits = leafRel().getTraitSet();

    final Stage currentStage = stage();

    switch (currentStage) {
      case AGGREGATE:
      case AGGREGATE_PROJECT:
        final RelCollation collation = leafRelTraits.getTrait(RelCollationTraitDef.INSTANCE);
        if (plannerContext.featureAvailable(EngineFeature.GROUPBY_IMPLICITLY_SORTS)
            && (collation == null || collation.getFieldCollations().isEmpty())
            && aggregate.getGroupSets().size() == 1) {
          // Druid sorts by grouping keys when grouping. Add the collation.
          // Note: [aggregate.getGroupSets().size() == 1] above means that collation isn't added for GROUPING SETS.
          final List<RelFieldCollation> sortFields = new ArrayList<>();

          if (currentStage == Stage.AGGREGATE) {
            for (int i = 0; i < aggregate.getGroupCount(); i++) {
              sortFields.add(new RelFieldCollation(i));
            }
          } else {
            // AGGREGATE_PROJECT
            final List<RexNode> projectExprs = aggregateProject.getProjects();

            // Build a map of all Project exprs that are input refs. Project expr index -> dimension index.
            final Int2IntMap dimensionMapping = new Int2IntOpenHashMap();
            dimensionMapping.defaultReturnValue(-1);
            for (int i = 0; i < projectExprs.size(); i++) {
              RexNode projectExpr = projectExprs.get(i);
              if (projectExpr.isA(SqlKind.INPUT_REF)) {
                dimensionMapping.put(((RexInputRef) projectExpr).getIndex(), i);
              }
            }

            // Add collations for dimensions so long as they are all mappings.
            for (int i = 0; i < aggregate.getGroupCount(); i++) {
              final int mapping = dimensionMapping.applyAsInt(i);
              if (mapping >= 0) {
                sortFields.add(new RelFieldCollation(mapping));
              } else {
                // As soon as we see a non-mapping, stop adding.
                break;
              }
            }
          }

          return leafRelTraits.plus(convention).plus(RelCollations.of(sortFields));
        }
        // Fall through.

      default:
        return leafRelTraits.plus(convention);
    }
  }

  public DruidQuery build(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    return DruidQuery.fromPartialQuery(
        this,
        dataSource,
        sourceRowSignature,
        plannerContext,
        rexBuilder,
        finalizeAggregations,
        null
    );
  }

  public DruidQuery build(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations,
      @Nullable VirtualColumnRegistry virtualColumnRegistry
  )
  {
    return DruidQuery.fromPartialQuery(
        this,
        dataSource,
        sourceRowSignature,
        plannerContext,
        rexBuilder,
        finalizeAggregations,
        virtualColumnRegistry
    );
  }

  public boolean canAccept(final Stage stage)
  {
    return stage.canFollow(stage());
  }

  /**
   * Returns the stage corresponding to the rel at the end of the query. It will match the rel returned from
   * {@link #leafRel()}.
   *
   * @return stage
   */
  @SuppressWarnings("VariableNotUsedInsideIf")
  public Stage stage()
  {
    if (windowProject != null) {
      return Stage.WINDOW_PROJECT;
    } else if (window != null) {
      return Stage.WINDOW;
    } else if (sortProject != null) {
      return Stage.SORT_PROJECT;
    } else if (sort != null) {
      return Stage.SORT;
    } else if (aggregateProject != null) {
      return Stage.AGGREGATE_PROJECT;
    } else if (havingFilter != null) {
      return Stage.HAVING_FILTER;
    } else if (aggregate != null) {
      return Stage.AGGREGATE;
    } else if (selectProject != null) {
      return Stage.SELECT_PROJECT;
    } else if (whereFilter != null) {
      return Stage.WHERE_FILTER;
    } else {
      return Stage.SCAN;
    }
  }

  /**
   * Returns the rel at the end of the query. It will match the stage returned from {@link #stage()}.
   *
   * @return leaf rel
   */
  public RelNode leafRel()
  {
    final Stage currentStage = stage();

    switch (currentStage) {
      case WINDOW_PROJECT:
        return windowProject;
      case WINDOW:
        return window;
      case SORT_PROJECT:
        return sortProject;
      case SORT:
        return sort;
      case AGGREGATE_PROJECT:
        return aggregateProject;
      case HAVING_FILTER:
        return havingFilter;
      case AGGREGATE:
        return aggregate;
      case SELECT_PROJECT:
        return selectProject;
      case WHERE_FILTER:
        return whereFilter;
      case SCAN:
        return scan;
      default:
        throw new ISE("Unknown stage: %s", currentStage);
    }
  }

  /**
   * Estimates the per-row cost of running this query.
   */
  public double estimateCost()
  {
    double cost = CostEstimates.COST_BASE;

    // Account for the cost of post-scan expressions.
    if (getSelectProject() != null) {
      for (final RexNode rexNode : getSelectProject().getProjects()) {
        if (!rexNode.isA(SqlKind.INPUT_REF)) {
          cost += CostEstimates.COST_EXPRESSION;
        }
      }
    }

    if (getWhereFilter() != null) {
      // We assume filters are free and have a selectivity of CostEstimates.MULTIPLIER_FILTER. They aren't actually
      // free, but we want to encourage filters, so let's go with it.
      cost *= CostEstimates.MULTIPLIER_FILTER;
    }

    if (getAggregate() != null) {
      cost += CostEstimates.COST_DIMENSION * getAggregate().getGroupSet().size();
      cost += CostEstimates.COST_AGGREGATION * getAggregate().getAggCallList().size();
    }

    if (getSort() != null) {
      if (!getSort().collation.getFieldCollations().isEmpty()) {
        cost *= CostEstimates.MULTIPLIER_ORDER_BY;
      }

      if (getSort().fetch != null) {
        cost *= CostEstimates.MULTIPLIER_LIMIT;
      }
    }

    // Account for the cost of post-aggregation expressions.
    if (getAggregateProject() != null) {
      for (final RexNode rexNode : getAggregateProject().getProjects()) {
        if (!rexNode.isA(SqlKind.INPUT_REF)) {
          cost += CostEstimates.COST_EXPRESSION;
        }
      }
    }

    // Account for the cost of post-sort expressions.
    if (getSortProject() != null) {
      for (final RexNode rexNode : getSortProject().getProjects()) {
        if (!rexNode.isA(SqlKind.INPUT_REF)) {
          cost += CostEstimates.COST_EXPRESSION;
        }
      }
    }

    // Account for the cost of generating outputs.
    cost += CostEstimates.COST_OUTPUT_COLUMN * getRowType().getFieldCount();

    return cost;
  }

  private void validateStage(final Stage stage)
  {
    if (!canAccept(stage)) {
      throw new ISE("Cannot move from stage[%s] to stage[%s]", stage(), stage);
    }
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
    PartialDruidQuery that = (PartialDruidQuery) o;
    return Objects.equals(scan, that.scan)
           && Objects.equals(whereFilter, that.whereFilter)
           && Objects.equals(selectProject, that.selectProject)
           && Objects.equals(aggregate, that.aggregate)
           && Objects.equals(havingFilter, that.havingFilter)
           && Objects.equals(aggregateProject, that.aggregateProject)
           && Objects.equals(sort, that.sort)
           && Objects.equals(sortProject, that.sortProject)
           && Objects.equals(window, that.window)
           && Objects.equals(windowProject, that.windowProject);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        builderSupplier,
        scan,
        whereFilter,
        selectProject,
        aggregate,
        havingFilter,
        aggregateProject,
        sort,
        sortProject,
        window,
        windowProject
    );
  }

  @Override
  public String toString()
  {
    return "PartialDruidQuery{" +
           "scan=" + scan +
           ", whereFilter=" + whereFilter +
           ", selectProject=" + selectProject +
           ", aggregate=" + aggregate +
           ", havingFilter=" + havingFilter +
           ", aggregateProject=" + aggregateProject +
           ", sort=" + sort +
           ", sortProject=" + sortProject +
           ", window=" + window +
           ", windowProject=" + windowProject +
           '}';
  }
}
