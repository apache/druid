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

package org.apache.druid.sql.calcite.planner.querygen;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.PDQVertexFactory.PDQVertex;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import org.apache.druid.sql.calcite.rule.DruidLogicalValuesRule;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.InlineTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a DAG of {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention
 * to a native Druid query for execution.
 */
public class DruidQueryGenerator
{
  private final PlannerContext plannerContext;
  private final RelNode relRoot;
  private final RexBuilder rexBuilder;
  private final PDQVertexFactory vertexFactory ;

  public DruidQueryGenerator(PlannerContext plannerContext, RelNode relRoot, RexBuilder rexBuilder)
  {
    this.plannerContext = plannerContext;
    this.relRoot = relRoot;
    this.rexBuilder = rexBuilder;
    this.vertexFactory = new PDQVertexFactory(plannerContext,rexBuilder);
  }

  public DruidQuery buildQuery()
  {
    Vertex vertex = buildVertexFor(relRoot, true);

    return vertex.buildQuery(true);
  }

  private Vertex buildVertexFor(RelNode node, boolean isRoot)
  {
    List<Vertex> newInputs = new ArrayList<>();
    for (RelNode input : node.getInputs()) {
      newInputs.add(buildVertexFor(input, false));
    }
    Vertex vertex = processNodeWithInputs(node, newInputs, isRoot);
    return vertex;
  }

  private Vertex processNodeWithInputs(RelNode node, List<Vertex> newInputs, boolean isRoot)
  {
    if(false && node instanceof XInputProducer) {
      XInputProducer xInputProducer = (XInputProducer) node;
      xInputProducer.validate(newInputs, isRoot);
//      PDQVertexFactory.this.createVertex()
      return vertexFactory .createVertex(node);
    }
    if (node instanceof DruidTableScan) {
      return processTableScan((DruidTableScan) node);
    }
    if (node instanceof DruidValues) {
      return processValues((DruidValues) node);
    }
    if (node instanceof Union) {
      return processUnion((Union) node, newInputs, isRoot);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Vertex newVertex = inputVertex.mergeIntoDruidQuery(node, isRoot);
      if (newVertex != null) {
        return newVertex;
      }
      // FIXME
      inputVertex = vertexFactory .createVertex(node, inputVertex);
      newVertex = inputVertex.mergeIntoDruidQuery(node, false);
      if (newVertex != null) {
        return newVertex;
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Vertex processUnion(Union node, List<Vertex> inputs, boolean isRoot)
  {
    Preconditions.checkArgument(!isRoot, "Root level Union is not supported!");
    Preconditions.checkArgument(inputs.size() > 1, "Union needs multiple inputs");
    for (Vertex inputVertex : inputs) {
      if (!inputVertex.canUnwrapInput()) {
        throw DruidException
            .defensive("Union operand with non-trivial remapping is not supported [%s]", inputVertex);
      }
    }
    return vertexFactory .createVertex(
        PartialDruidQuery.create(node),
        inputs
    );
  }

  private Vertex processValues(DruidValues values)
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

    PDQVertex vertex = vertexFactory .createVertex(values);
    vertex.currentTable = inlineTable;

    return createTableScanVertex(values, inlineTable, null);

  }

  private Vertex processTableScan(DruidTableScan scan)
  {
    if (!(scan instanceof DruidTableScan)) {
      throw new ISE("Planning hasn't converted logical table scan to druid convention");
    }
    DruidTableScan druidTableScan = scan;
    Preconditions.checkArgument(scan.getInputs().size() == 0);


    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);

    Preconditions.checkArgument(druidTable != null);

    Project project = druidTableScan.getProject();

    return createTableScanVertex(scan, druidTable, project);
  }

  private Vertex createTableScanVertex(RelNode scan, final DruidTable druidTable, Project project)
  {
    PDQVertex vertex = vertexFactory .createVertex(scan);
    vertex.currentTable = druidTable;
    if (project != null) {
      vertex.partialDruidQuery = vertex.partialDruidQuery.withSelectProject(project);
    }
    return vertex;
  }
}
