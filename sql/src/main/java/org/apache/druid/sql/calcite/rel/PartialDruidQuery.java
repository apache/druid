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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

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

  public enum Stage
  {
    // SCAN must be present on all queries.
    SCAN,

    // WHERE_FILTER, SELECT_PROJECT may be present on any query.
    WHERE_FILTER,
    SELECT_PROJECT,

    // AGGREGATE, HAING_FILTER, AGGREGATE_PROJECT can only be present on aggregating queries.
    AGGREGATE,
    HAVING_FILTER,
    AGGREGATE_PROJECT,

    // SORT, SORT_PROJECT may be present on any query.
    SORT,
    SORT_PROJECT
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
      final Project sortProject
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
  }

  public static PartialDruidQuery create(final RelNode scanRel)
  {
    final Supplier<RelBuilder> builderSupplier = () -> RelFactories.LOGICAL_BUILDER.create(
        scanRel.getCluster(),
        scanRel.getTable().getRelOptSchema()
    );
    return new PartialDruidQuery(builderSupplier, scanRel, null, null, null, null, null, null, null);
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
        sortProject
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
            newSelectProject.getRowType().getFieldNames()
        );
        theProject = (Project) relBuilder.build();
      }
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
        sortProject
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
        sortProject
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
        sortProject
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
        sortProject
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
        sortProject
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
        newSortProject
    );
  }

  public RelDataType getRowType()
  {
    return leafRel().getRowType();
  }

  public RelTrait[] getRelTraits()
  {
    return leafRel().getTraitSet().toArray(new RelTrait[0]);
  }

  public DruidQuery build(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    return new DruidQuery(this, dataSource, sourceRowSignature, plannerContext, rexBuilder, finalizeAggregations);
  }

  public boolean canAccept(final Stage stage)
  {
    final Stage currentStage = stage();

    if (currentStage == Stage.SELECT_PROJECT && stage == Stage.SELECT_PROJECT) {
      // Special case: allow layering SELECT_PROJECT on top of SELECT_PROJECT. Calcite's builtin rules cannot
      // always collapse these, so we have to (one example: testSemiJoinWithOuterTimeExtract). See
      // withSelectProject for the code here that handles this.
      return true;
    } else if (stage.compareTo(currentStage) <= 0) {
      // Cannot go backwards.
      return false;
    } else if (stage.compareTo(Stage.AGGREGATE) > 0 && stage.compareTo(Stage.SORT) < 0 && aggregate == null) {
      // Cannot do post-aggregation stages without an aggregation.
      return false;
    } else if (stage.compareTo(Stage.SORT) > 0 && sort == null) {
      // Cannot do post-sort stages without a sort.
      return false;
    } else {
      // Looks good.
      return true;
    }
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
    if (sortProject != null) {
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
        throw new ISE("WTF?! Unknown stage: %s", currentStage);
    }
  }

  private void validateStage(final Stage stage)
  {
    if (!canAccept(stage)) {
      throw new ISE("Cannot move from stage[%s] to stage[%s]", stage(), stage);
    }
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PartialDruidQuery that = (PartialDruidQuery) o;
    return Objects.equals(scan, that.scan) &&
           Objects.equals(whereFilter, that.whereFilter) &&
           Objects.equals(selectProject, that.selectProject) &&
           Objects.equals(aggregate, that.aggregate) &&
           Objects.equals(havingFilter, that.havingFilter) &&
           Objects.equals(aggregateProject, that.aggregateProject) &&
           Objects.equals(sort, that.sort) &&
           Objects.equals(sortProject, that.sortProject);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        scan,
        whereFilter,
        selectProject,
        aggregate,
        havingFilter,
        aggregateProject,
        sort,
        sortProject
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
           '}';
  }
}
