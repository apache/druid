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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.PDQVertexFactory.PDQVertex;
import org.apache.druid.sql.calcite.planner.querygen.InputDescProducer.InputDesc;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery.Stage;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a DAG of {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention to a native
 * {@link DruidQuery} for execution.
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
      return vertexFactory.createVertex(PartialDruidQuery.create(node), newInputs);
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
      inputVertex = vertexFactory.createVertex(
          PartialDruidQuery.createOuterQuery(((PDQVertex) inputVertex).partialDruidQuery),
          ImmutableList.of(inputVertex)
      );
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

  /**
   * Execution dag vertex - encapsulates a list of operators.
   */
  private interface Vertex
  {
    /**
     * Builds the query.
     */
    DruidQuery buildQuery(boolean isRoot);

    /**
     * Merges the given node into the current {@link Vertex}
     *
     * @return the new {@link Vertex} or <code>null</code> if merging is not possible.
     */
    @Nullable
    Vertex mergeNode(RelNode node, boolean isRoot);

    /**
     * Decides wether this {@link Vertex} can be unwrapped into an {@link InputDesc}.
     */
    boolean canUnwrapInput();

    /**
     * Unwraps this {@link Vertex} into an {@link InputDesc}.
     *
     * Unwraps the input of this vertex - if it doesn't do anything beyond reading its input.
     *
     * @throws DruidException if unwrap is not possible.
     */
    InputDesc unwrapInputDesc();
  }

  /**
   * {@link PartialDruidQuery} based {@link Vertex} factory.
   */
  protected static class PDQVertexFactory
  {
    private final PlannerContext plannerContext;
    private final RexBuilder rexBuilder;

    public PDQVertexFactory(PlannerContext plannerContext, RexBuilder rexBuilder)
    {
      this.plannerContext = plannerContext;
      this.rexBuilder = rexBuilder;
    }

    Vertex createVertex(PartialDruidQuery partialDruidQuery, List<Vertex> inputs)
    {
      return new PDQVertex(partialDruidQuery, inputs);
    }

    public class PDQVertex implements Vertex
    {
      final PartialDruidQuery partialDruidQuery;
      final List<Vertex> inputs;

      public PDQVertex(PartialDruidQuery partialDruidQuery, List<Vertex> inputs)
      {
        this.partialDruidQuery = partialDruidQuery;
        this.inputs = inputs;
      }

      @Override
      public DruidQuery buildQuery(boolean topLevel)
      {
        InputDesc input = getInput();
        return partialDruidQuery.build(
            input.dataSource,
            input.rowSignature,
            plannerContext,
            rexBuilder,
            !topLevel
        );
      }

      /**
       * Creates the {@link InputDesc} for the current {@link Vertex}.
       */
      private InputDesc getInput()
      {
        List<InputDesc> inputDescs = new ArrayList<>();
        for (Vertex inputVertex : inputs) {
          final InputDesc desc;
          if (inputVertex.canUnwrapInput()) {
            desc = inputVertex.unwrapInputDesc();
          } else {
            DruidQuery inputQuery = inputVertex.buildQuery(false);
            desc = new InputDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());
          }
          inputDescs.add(desc);
        }
        RelNode scan = partialDruidQuery.getScan();
        if (scan instanceof InputDescProducer) {
          InputDescProducer inp = (InputDescProducer) scan;
          return inp.getInputDesc(plannerContext, inputDescs);
        }
        if (inputs.size() == 1) {
          return inputDescs.get(0);
        }
        throw DruidException.defensive("Unable to create InputDesc for Operator [%s]", scan);
      }

      /**
       * Merges the given {@link RelNode} into the current partial query.
       *
       * @return the new merged vertex - or null if its not possible to merge
       */
      @Override
      @Nullable
      public Vertex mergeNode(RelNode node, boolean isRoot)
      {
        PartialDruidQuery newPartialQuery = mergeNode1(node, isRoot);
        if (newPartialQuery == null) {
          return null;
        }
        return createVertex(newPartialQuery, inputs);
      }

      /**
       * Merges the given {@link RelNode} into the current {@link PartialDruidQuery}.
       *
       * @return the new merged {@link PartialDruidQuery} - or null if its not possible to merge.
       */
      @Nullable
      public PartialDruidQuery mergeNode1(RelNode node, boolean isRoot)
      {
        if (accepts(node, Stage.WHERE_FILTER, Filter.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter((Filter) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.SELECT_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSelectProject((Project) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.AGGREGATE, Aggregate.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregate((Aggregate) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.AGGREGATE_PROJECT, Project.class) && isRoot) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregateProject((Project) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.HAVING_FILTER, Filter.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter((Filter) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.SORT, Sort.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSort((Sort) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.SORT_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSortProject((Project) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.WINDOW, Window.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWindow((Window) node);
          return newPartialQuery;
        }
        if (accepts(node, Stage.WINDOW_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWindowProject((Project) node);
          return newPartialQuery;
        }
        return null;
      }

      private boolean accepts(RelNode node, Stage whereFilter, Class<? extends RelNode> class1)
      {
        return partialDruidQuery.canAccept(whereFilter) && class1.isInstance(node);
      }

      @Override
      public InputDesc unwrapInputDesc()
      {
        if (canUnwrapInput()) {
          DruidQuery q = buildQuery(false);
          InputDesc origInput = getInput();
          return new InputDesc(origInput.dataSource, q.getOutputRowSignature());
        }
        throw DruidException.defensive("Can't unwrap input of vertex[%s]", partialDruidQuery);
      }

      @Override
      public boolean canUnwrapInput()
      {
        if (partialDruidQuery.stage() == Stage.SCAN) {
          return true;
        }
        if (partialDruidQuery.stage() == PartialDruidQuery.Stage.SELECT_PROJECT &&
            partialDruidQuery.getWhereFilter() == null &&
            partialDruidQuery.getSelectProject().isMapping()) {
          return true;
        }
        return false;
      }
    }

  }
}
