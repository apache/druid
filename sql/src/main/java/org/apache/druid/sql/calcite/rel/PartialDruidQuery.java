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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;

import java.util.Objects;

/**
 * Builder for a Druid query, not counting the "dataSource" (which will be slotted in later).
 */
public class PartialDruidQuery
{
  private final RelNode scan;
  private final Filter whereFilter;
  private final Project selectProject;
  private final Sort selectSort;
  private final Aggregate aggregate;
  private final Filter havingFilter;
  private final Project aggregateProject;
  private final Sort sort;
  private final Project sortProject;

  public enum Stage
  {
    SCAN,
    WHERE_FILTER,
    SELECT_PROJECT,
    SELECT_SORT,
    AGGREGATE,
    HAVING_FILTER,
    AGGREGATE_PROJECT,
    SORT,
    SORT_PROJECT
  }

  public PartialDruidQuery(
      final RelNode scan,
      final Filter whereFilter,
      final Project selectProject,
      final Sort selectSort,
      final Aggregate aggregate,
      final Project aggregateProject,
      final Filter havingFilter,
      final Sort sort,
      final Project sortProject
  )
  {
    this.scan = Preconditions.checkNotNull(scan, "scan");
    this.whereFilter = whereFilter;
    this.selectProject = selectProject;
    this.selectSort = selectSort;
    this.aggregate = aggregate;
    this.aggregateProject = aggregateProject;
    this.havingFilter = havingFilter;
    this.sort = sort;
    this.sortProject = sortProject;
  }

  public static PartialDruidQuery create(final RelNode scanRel)
  {
    return new PartialDruidQuery(scanRel, null, null, null, null, null, null, null, null);
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

  public Sort getSelectSort()
  {
    return selectSort;
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
        scan,
        newWhereFilter,
        selectProject,
        selectSort,
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
    return new PartialDruidQuery(
        scan,
        whereFilter,
        newSelectProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withSelectSort(final Sort newSelectSort)
  {
    validateStage(Stage.SELECT_SORT);
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        newSelectSort,
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
        scan,
        whereFilter,
        selectProject,
        selectSort,
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
        scan,
        whereFilter,
        selectProject,
        selectSort,
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
        scan,
        whereFilter,
        selectProject,
        selectSort,
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
        scan,
        whereFilter,
        selectProject,
        selectSort,
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
        scan,
        whereFilter,
        selectProject,
        selectSort,
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

    if (stage.compareTo(currentStage) <= 0) {
      // Cannot go backwards.
      return false;
    } else if (stage.compareTo(Stage.AGGREGATE) > 0 && aggregate == null) {
      // Cannot do post-aggregation stages without an aggregation.
      return false;
    } else if (stage.compareTo(Stage.AGGREGATE) >= 0 && selectSort != null) {
      // Cannot do any aggregations after a select + sort.
      return false;
    } else if (stage.compareTo(Stage.SORT) > 0 && sort == null) {
      // Cannot add sort project without a sort
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
    } else if (selectSort != null) {
      return Stage.SELECT_SORT;
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
      case SELECT_SORT:
        return selectSort;
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
           Objects.equals(selectSort, that.selectSort) &&
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
        selectSort,
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
           ", selectSort=" + selectSort +
           ", aggregate=" + aggregate +
           ", havingFilter=" + havingFilter +
           ", aggregateProject=" + aggregateProject +
           ", sort=" + sort +
           ", sortProject=" + sortProject +
           '}';
  }
}
