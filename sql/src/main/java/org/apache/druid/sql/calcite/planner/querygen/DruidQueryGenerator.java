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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a DAG of
 * {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention
 * to a native {@link DruidQuery} for execution.
 */
public class DruidQueryGenerator
{
  private final RelNode relRoot;
  private final PDQVertexFactory vertexFactory;

  public DruidQueryGenerator(PlannerContext plannerContext, RelNode relRoot, RexBuilder rexBuilder)
  {
    this.relRoot = relRoot;
    this.vertexFactory = new PDQVertexFactory(plannerContext, rexBuilder);
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
    if (node instanceof InputDescProducer) {
      InputDescProducer in = (InputDescProducer) node;
      // ensure that inputDesc is available (checks should happen in this method)
      in.getInputDesc(vertexFactory.getPlannerContext());
      return vertexFactory.createVertex(node);
    }
    if (node instanceof Union) {
      return processUnion((Union) node, newInputs);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Vertex newVertex = inputVertex.mergeNode(node, isRoot);
      if (newVertex != null) {
        return newVertex;
      }
      // FIXME
      inputVertex = vertexFactory.createVertex(node, inputVertex);
      newVertex = inputVertex.mergeNode(node, false);
      if (newVertex != null) {
        return newVertex;
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Vertex processUnion(Union node, List<Vertex> inputs)
  {
    Preconditions.checkArgument(inputs.size() > 1, "Union needs multiple inputs");
    for (Vertex inputVertex : inputs) {
      if (!inputVertex.canUnwrapInput()) {
        throw DruidException
            .defensive("Union operand with non-trivial remapping is not supported [%s]", inputVertex);
      }
    }
    return vertexFactory.createVertex(
        PartialDruidQuery.create(node),
        inputs
    );
  }
}
