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

import com.google.common.base.Preconditions;
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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery.Stage;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import org.apache.druid.sql.calcite.rule.DruidLogicalValuesRule;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.InlineTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converts a DAG of
 * {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention
 * to a native Druid query for execution. The convertion is done via a
 * {@link org.apache.calcite.rel.RelShuttle} visitor implementation.
 * FIXME
 */
public class DruidQueryGenerator extends RelShuttleImpl
{
  private final List<PartialDruidQuery> queryList = new ArrayList<>();
  private final List<DruidTable> queryTables = new ArrayList<>();
  private final PlannerContext plannerContext;
  private PartialDruidQuery partialDruidQuery;
  private PartialDruidQuery.Stage currentStage = null;
  private DruidTable currentTable = null;
  private final RelNode relRoot;
  private final RexBuilder rexBuilder;

//  static final List<Vertex> vertices = new ArrayList<Vertex>();
//  static Vertex root;

  static class InputDesc {

    private DataSource dataSource;
    private RowSignature rowSignature;

    public InputDesc(DataSource dataSource, RowSignature rowSignature)
    {
      this.dataSource = dataSource;
      this.rowSignature = rowSignature;

    }

  }
  public Vertex createVertex(TableScan scan)
  {
    // FIXME
    Vertex vertex = new Vertex();
    vertex.partialDruidQuery = PartialDruidQuery.create(scan);
    return vertex;
  }
  public Vertex createVertex(Values scan)
  {
    // FIXME
    Vertex vertex = new Vertex();
    vertex.partialDruidQuery = PartialDruidQuery.create(scan);
    return vertex;
  }


  // FIXME
  private Vertex createVertex(RelNode node, Vertex inputVertex)
  {
    // FIXME
    Vertex vertex = new Vertex();
    vertex.inputs = ImmutableList.of(inputVertex);
    vertex.partialDruidQuery = PartialDruidQuery.createOuterQuery(inputVertex.partialDruidQuery);
    return vertex;
  }




  class Vertex {
    PartialDruidQuery partialDruidQuery;
    DruidTable queryTable;
    List<Vertex> inputs;
    public DruidTable currentTable;



    public DruidQuery buildQuery(boolean topLevel)
    {
      InputDesc input= getInput();
      return partialDruidQuery.build(
          input.dataSource,
          input.rowSignature,
          plannerContext,
          rexBuilder,
          currentTable != null && !topLevel
      );
    }

    private InputDesc getInput()
    {
      if(currentTable!=null) {
        return new InputDesc(currentTable.getDataSource(),currentTable.getRowSignature());
      }

      if(inputs.size()==1) {
        DruidQuery inputQuery = inputs.get(0).buildQuery(false);

        return new InputDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());

      }
      throw new IllegalStateException();
    }

    public Optional<Vertex> mergeFilter(Filter filter)
    {
      if(partialDruidQuery.canAccept(Stage.WHERE_FILTER)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter(filter);
        return Optional.of(newVertex(newPartialQuery));
      }
      if(partialDruidQuery.canAccept(Stage.HAVING_FILTER)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter(filter);
        return Optional.of(newVertex(newPartialQuery));
      }
      return Optional.empty();
    }

    private Vertex newVertex(PartialDruidQuery partialDruidQuery)
    {
      Vertex vertex = new Vertex();
      vertex.inputs = inputs;
      vertex.currentTable = currentTable;
      vertex.queryTable = queryTable;
      vertex.partialDruidQuery = partialDruidQuery;
      return vertex;

    }

    private Vertex mergeIntoDruidQuery(RelNode node, boolean isRoot)
    {
////  final RelNode scan,
////  final Filter whereFilter,
      if(accepts(node, Stage.WHERE_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
////  final Project selectProject,
      if(accepts(node, Stage.SELECT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSelectProject((Project) node);
        return newVertex(newPartialQuery);
      }
////  final Aggregate aggregate,
      if(accepts(node, Stage.AGGREGATE, Aggregate.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregate((Aggregate) node);
        return newVertex(newPartialQuery);
      }
////  final Project aggregateProject,
      if(accepts(node, Stage.AGGREGATE_PROJECT, Project.class) && isRoot) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregateProject((Project) node);
        return newVertex(newPartialQuery);
      }
////  final Filter havingFilter,
      if(accepts(node, Stage.HAVING_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
////  final Sort sort,
      if(accepts(node, Stage.SORT, Sort.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSort((Sort) node);
        return newVertex(newPartialQuery);
      }
////  final Project sortProject,
      if(accepts(node, Stage.SORT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSortProject((Project) node);
        return newVertex(newPartialQuery);
      }
////  final Window window,
      if(accepts(node, Stage.WINDOW, Window.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindow((Window) node);
        return newVertex(newPartialQuery);
      }
////  final Project windowProject
      if(accepts(node, Stage.WINDOW_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindowProject((Project) node);
        return newVertex(newPartialQuery);
      }


      return null;

    }

    private boolean accepts(RelNode node, Stage whereFilter, Class<? extends RelNode> class1)
    {
      return partialDruidQuery.canAccept(whereFilter) && class1.isInstance(node);
    }
  }

  public DruidQueryGenerator(PlannerContext plannerContext, RelNode relRoot, RexBuilder rexBuilder)
  {
    this.plannerContext = plannerContext;
    this.relRoot = relRoot;
    this.rexBuilder=rexBuilder;
  }


  public Vertex buildVertex()
  {
    return buildVertexFor(relRoot, true);
  }

  protected Vertex buildVertexFor(RelNode node, boolean isRoot)
  {
    Preconditions.checkArgument(node.getInputs().size() <= 1, "unsupported");

    List<Vertex> newInputs = new ArrayList<>();
    for (RelNode input : node.getInputs()) {
      newInputs.add(buildVertexFor(input, false));
    }

    Vertex vertex = processNodeWithInputs(node, newInputs, isRoot);

    return vertex;
  }


  private Vertex processNodeWithInputs(RelNode node, List<Vertex> newInputs, boolean isRoot)
  {
    if(node instanceof TableScan) {
      return visitTableScan((TableScan) node);
    }
    if(node instanceof DruidValues) {
      return visitValues1((DruidValues)node);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Vertex newVertex = inputVertex.mergeIntoDruidQuery(node, isRoot);
      if (newVertex != null) {
        return newVertex;
      }
      inputVertex = createVertex(node, inputVertex);
      newVertex = inputVertex.mergeIntoDruidQuery(node, false);
      if (newVertex != null) {
        return newVertex;
      } else {
        throw new IllegalStateException("should not happend");
      }

    }
    //
//
////    final RelNode scan,
////    final Filter whereFilter,
////    final Project selectProject,
////    final Aggregate aggregate,
////    final Project aggregateProject,
////    final Filter havingFilter,
////    final Sort sort,
////    final Project sortProject,
////    final Window window,
////    final Project windowProject
//
//    if(node instanceof Filter) {
//      Preconditions.checkArgument(newInputs.size() == 1);
//      return visitFilter2((Filter) node, newInputs.get(0));
//    }
//    if(node instanceof Sort) {
//      return newInputs.get(0);
//    }
    throw new UnsupportedOperationException();
  }



  private Vertex visitValues1(DruidValues values)
  {
      final List<ImmutableList<RexLiteral>> tuples = values.getTuples();
      final List<Object[]> objectTuples = tuples
          .stream()
          .map(
              tuple -> tuple
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
      InlineTable inlineTable = new InlineTable(InlineDataSource.fromIterable(objectTuples, rowSignature));


      Vertex vertex = createVertex(values);
      vertex.currentTable=inlineTable;

      return vertex;

  }



  public Vertex visitTableScan(TableScan scan)
  {
    if (!(scan instanceof DruidTableScan)) {
      throw new ISE("Planning hasn't converted logical table scan to druid convention");
    }
    DruidTableScan druidTableScan = (DruidTableScan) scan;
    Preconditions.checkArgument(scan.getInputs().size() == 0);

    Vertex vertex = createVertex(scan);

    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);

    Preconditions.checkArgument(druidTable != null);

    vertex.currentTable = druidTable;
    if (druidTableScan.getProject() != null) {
      //FIXME ? XXX
      vertex.partialDruidQuery = vertex.partialDruidQuery.withSelectProject(druidTableScan.getProject());
      //      currentStage = PartialDruidQuery.Stage.SELECT_PROJECT;
    }
    return vertex;
  }



  @Override
  public RelNode visit(TableScan scan)
  {
    if (!(scan instanceof DruidTableScan)) {
      throw new ISE("Planning hasn't converted logical table scan to druid convention");
    }
    DruidTableScan druidTableScan = (DruidTableScan) scan;
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
    RelNode result = super.visit(values);
    final List<ImmutableList<RexLiteral>> tuples = values.getTuples();
    final List<Object[]> objectTuples = tuples
        .stream()
        .map(
            tuple -> tuple
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


  public Vertex visitFilter2(Filter filter, Vertex vertex)
  {
    PartialDruidQuery.Stage currentStage  = vertex.partialDruidQuery.stage();

    Optional<Vertex> newVertex = vertex.mergeFilter(filter);
    if (newVertex.isPresent()) {
      return newVertex.get();
    }
    return createVertex(filter, vertex);
  }

  public RelNode visitFilter(Filter filter)
  {
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
    RelNode result = super.visit(project);
    if (isRoot(project) && (currentStage == PartialDruidQuery.Stage.AGGREGATE
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
    } else if (other instanceof Window) {
      return visitWindow((Window) other);
    } else if (other instanceof Union) {
      return visitUnion((Union) other);
    }

    throw new UOE("Found unsupported RelNode [%s]", other.getClass().getSimpleName());
  }

  private RelNode visitUnion(Union union)
  {
    RelNode newUnion = super.visit(union);

    return null;
  }

  private RelNode visitWindow(Window other)
  {
    RelNode result = super.visit(other);
    if (!PartialDruidQuery.Stage.WINDOW.canFollow(currentStage)) {
      queryList.add(partialDruidQuery);
      queryTables.add(currentTable);
      partialDruidQuery = PartialDruidQuery.createOuterQuery(partialDruidQuery);
    }
    partialDruidQuery = partialDruidQuery.withWindow((Window) result);
    currentStage = PartialDruidQuery.Stage.WINDOW;

    return result;
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

  public void run()
  {
    relRoot.accept(this);
  }

  public boolean isRoot(RelNode node)
  {
    return node == relRoot;
  }
}
