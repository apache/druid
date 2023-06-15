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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rule.DruidLogicalValuesRule;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.InlineTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Converts a DAG of {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention to a native
 * Druid query for execution. The convertion is done via a {@link org.apache.calcite.rel.RelShuttle} visitor
 * implementation.
 */
public class DruidQueryGenerator extends RelShuttleImpl
{
  private final List<PartialDruidQuery> queryList = new ArrayList<>();
  private final List<DruidTable> queryTables = new ArrayList<>();
  private final PlannerContext plannerContext;
  private PartialDruidQuery partialDruidQuery;
  private PartialDruidQuery.Stage currentStage = null;
  private DruidTable currentTable = null;
  private boolean isRoot = true;

  public DruidQueryGenerator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public RelNode visit(TableScan scan)
  {
    if (!(scan instanceof DruidTableScan)) {
      throw new ISE("Planning hasn't converted logical table scan to druid convention");
    }
    DruidTableScan druidTableScan = (DruidTableScan) scan;
    isRoot = false;
    RelNode result = super.visit(scan);
    partialDruidQuery = PartialDruidQuery.create(scan);
    currentStage = PartialDruidQuery.Stage.SCAN;
    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);
    if (druidTable != null) {
      currentTable = druidTable;
    }
    if (druidTableScan.getProject() != null) {
      partialDruidQuery = partialDruidQuery.withSelectProject(druidTableScan.getProject());
      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    }
    return result;
  }

  @Override
  public RelNode visit(TableFunctionScan scan)
  {
    return null;
  }

  @Override
  public RelNode visit(LogicalValues values)
  {
    isRoot = false;
    RelNode result = super.visit(values);
    final List<ImmutableList<RexLiteral>> tuples = values.getTuples();
    final List<Object[]> objectTuples = tuples
        .stream()
        .map(tuple -> tuple
            .stream()
            .map(v -> DruidLogicalValuesRule.getValueFromLiteral(v, plannerContext))
            .collect(Collectors.toList())
            .toArray(new Object[0])
        )
        .collect(Collectors.toList());
    RowSignature rowSignature = RowSignatures.fromRelDataType(
        values.getRowType().getFieldNames(),
        values.getRowType()
    );
    currentTable = new InlineTable(InlineDataSource.fromIterable(objectTuples, rowSignature));
    if (currentStage == null) {
      partialDruidQuery = PartialDruidQuery.create(values);
      currentStage = PartialDruidQuery.Stage.SCAN;
    } else {
      throw new ISE("Values node found at non leaf node in the plan");
    }
    return result;
  }

  @Override
  public RelNode visit(LogicalFilter filter)
  {
    return visitFilter(filter);
  }

  public RelNode visitFilter(Filter filter)
  {
    isRoot = false;
    RelNode result = super.visit(filter);
    if (currentStage == PartialDruidQuery.Stage.AGGREGATE) {
      partialDruidQuery = partialDruidQuery.withHavingFilter(filter);
      currentStage = PartialDruidQuery.Stage.HAVING_FILTER;
    } else if (currentStage == PartialDruidQuery.Stage.SCAN) {
      partialDruidQuery = partialDruidQuery.withWhereFilter(filter);
      currentStage = PartialDruidQuery.Stage.WHERE_FILTER;
    } else if (currentStage == PartialDruidQuery.Stage.SELECT_PROJECT) {
      PartialDruidQuery old = partialDruidQuery;
      partialDruidQuery = PartialDruidQuery.create(old.getScan());
      partialDruidQuery = partialDruidQuery.withWhereFilter(filter);
      partialDruidQuery = partialDruidQuery.withSelectProject(old.getSelectProject());
      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    } else {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery).withWhereFilter(filter);
      currentStage = PartialDruidQuery.Stage.WHERE_FILTER;
    }
    return result;
  }

  @Override
  public RelNode visit(LogicalProject project)
  {
    return visitProject(project);
  }

  @Override
  public RelNode visit(LogicalJoin join)
  {
    throw new UnsupportedOperationException("Found join");
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate)
  {
    return null;
  }

  @Override
  public RelNode visit(LogicalUnion union)
  {
    throw new UnsupportedOperationException("Found union");
  }

  @Override
  public RelNode visit(LogicalIntersect intersect)
  {
    return null;
  }

  @Override
  public RelNode visit(LogicalMinus minus)
  {
    return null;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate)
  {
    isRoot = false;
    RelNode result = super.visit(aggregate);
    if (PartialDruidQuery.Stage.AGGREGATE.canFollow(currentStage)) {
      partialDruidQuery = partialDruidQuery.withAggregate(aggregate);
    } else {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery).withAggregate(aggregate);
    }
    currentStage = PartialDruidQuery.Stage.AGGREGATE;
    return result;
  }

  @Override
  public RelNode visit(LogicalMatch match)
  {
    return null;
  }

  @Override
  public RelNode visit(LogicalSort sort)
  {
    return visitSort(sort);
  }

  @Override
  public RelNode visit(LogicalExchange exchange)
  {
    return null;
  }

  private RelNode visitProject(Project project)
  {
    boolean rootForReal = isRoot;
    isRoot = false;
    RelNode result = super.visit(project);
    if (rootForReal && (currentStage == PartialDruidQuery.Stage.AGGREGATE
                        || currentStage == PartialDruidQuery.Stage.HAVING_FILTER)) {
      partialDruidQuery = partialDruidQuery.withAggregateProject(project);
      currentStage = PartialDruidQuery.Stage.AGGREGATE_PROJECT;
    } else if (currentStage == PartialDruidQuery.Stage.SCAN || currentStage == PartialDruidQuery.Stage.WHERE_FILTER) {
      partialDruidQuery = partialDruidQuery.withSelectProject(project);
      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    } else if (currentStage == PartialDruidQuery.Stage.SELECT_PROJECT) {
      partialDruidQuery = partialDruidQuery.mergeProject(project);
      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    } else if (currentStage == PartialDruidQuery.Stage.SORT) {
      partialDruidQuery = partialDruidQuery.withSortProject(project);
      currentStage = PartialDruidQuery.Stage.SORT_PROJECT;
    } else {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery).withSelectProject(project);
      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    }
    return result;
  }

  private RelNode visitSort(Sort sort)
  {
    isRoot = false;
    RelNode result = super.visit(sort);
    if (PartialDruidQuery.Stage.SORT.canFollow(currentStage)) {
      partialDruidQuery = partialDruidQuery.withSort(sort);
    } else {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery).withSort(sort);
    }
    currentStage = PartialDruidQuery.Stage.SORT;
    return result;
  }

  private RelNode visitAggregate(Aggregate aggregate)
  {
    isRoot = false;
    RelNode result = super.visit(aggregate);
    if (PartialDruidQuery.Stage.AGGREGATE.canFollow(currentStage)) {
      partialDruidQuery = partialDruidQuery.withAggregate(aggregate);
    } else {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery).withAggregate(aggregate);
    }
    currentStage = PartialDruidQuery.Stage.AGGREGATE;
    return result;
  }

  @Override
  public RelNode visit(RelNode other)
  {
    if (other instanceof TableScan) {
      return visit((TableScan) other);
    } else if (other instanceof Project) {
      return visitProject((Project) other);
    } else if (other instanceof Sort) {
      return visitSort((Sort) other);
    } else if (other instanceof Aggregate) {
      return visitAggregate((Aggregate) other);
    } else if (other instanceof Filter) {
      return visitFilter((Filter) other);
    } else if (other instanceof LogicalValues) {
      return visit((LogicalValues) other);
    }

    return super.visit(other);
  }

  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialDruidQuery;
  }

  public List<PartialDruidQuery> getQueryList()
  {
    return queryList;
  }

  public List<DruidTable> getQueryTables()
  {
    return queryTables;
  }

  public DruidTable getCurrentTable()
  {
    return currentTable;
  }

}
