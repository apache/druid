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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.PDQVertexFactory.PDQVertex;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery.Stage;
import org.apache.druid.sql.calcite.rel.logical.DruidAggregate;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidSort;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/**
 * Converts a DAG of {@link DruidLogicalNode} convention to a native {@link DruidQuery} for execution.
 */
public class DruidQueryGenerator
{
  private final DruidLogicalNode relRoot;
  private final PDQVertexFactory vertexFactory;

  public DruidQueryGenerator(PlannerContext plannerContext, DruidLogicalNode relRoot, RexBuilder rexBuilder)
  {
    this.relRoot = relRoot;
    this.vertexFactory = new PDQVertexFactory(plannerContext, rexBuilder);
  }

  public DruidQuery buildQuery()
  {
    Stack<DruidLogicalNode> stack = new Stack<>();
    stack.push(relRoot);
    Vertex vertex = buildVertexFor(stack, true);
    return vertex.buildQuery(true);
  }

  private Vertex buildVertexFor(Stack<DruidLogicalNode> stack, boolean isRoot)
  {
    List<Vertex> newInputs = new ArrayList<>();

    for (RelNode input : stack.peek().getInputs()) {
      stack.push((DruidLogicalNode) input);
      newInputs.add(buildVertexFor(stack, false));
      stack.pop();
    }
    Vertex vertex = processNodeWithInputs(stack, newInputs, isRoot);
    return vertex;
  }

  private Vertex processNodeWithInputs(Stack<DruidLogicalNode> stack, List<Vertex> newInputs, boolean isRoot)
  {
    DruidLogicalNode node = stack.peek();
    if (node instanceof SourceDescProducer) {
      return vertexFactory.createVertex(PartialDruidQuery.create(node), newInputs);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Optional<Vertex> newVertex = inputVertex.extendWith(stack, isRoot);
      if (newVertex.isPresent()) {
        return newVertex.get();
      }
      inputVertex = vertexFactory.createVertex(
          PartialDruidQuery.createOuterQuery(((PDQVertex) inputVertex).partialDruidQuery),
          ImmutableList.of(inputVertex)
      );
      newVertex = inputVertex.extendWith(stack, false);
      if (newVertex.isPresent()) {
        return newVertex.get();
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
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
     * Extends the current vertex to include the specified parent.
     */
    Optional<Vertex> extendWith(Stack<DruidLogicalNode> stack, boolean isRoot);

    /**
     * Decides wether this {@link Vertex} can be unwrapped into an {@link SourceDesc}.
     */
    boolean canUnwrapSourceDesc();

    /**
     * Unwraps this {@link Vertex} into an {@link SourceDesc}.
     *
     * Unwraps the source of this vertex - if it doesn't do anything beyond reading its input.
     *
     * @throws DruidException if unwrap is not possible.
     */
    SourceDesc unwrapSourceDesc();
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
        SourceDesc source = getSource();
        return partialDruidQuery.build(
            source.dataSource,
            source.rowSignature,
            plannerContext,
            rexBuilder,
            !topLevel
        );
      }

      /**
       * Creates the {@link SourceDesc} for the current {@link Vertex}.
       */
      private SourceDesc getSource()
      {
        List<SourceDesc> sourceDescs = new ArrayList<>();
        for (Vertex inputVertex : inputs) {
          final SourceDesc desc;
          if (inputVertex.canUnwrapSourceDesc()) {
            desc = inputVertex.unwrapSourceDesc();
          } else {
            DruidQuery inputQuery = inputVertex.buildQuery(false);
            desc = new SourceDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());
          }
          sourceDescs.add(desc);
        }
        RelNode scan = partialDruidQuery.getScan();
        if (scan instanceof SourceDescProducer) {
          SourceDescProducer inp = (SourceDescProducer) scan;
          return inp.getSourceDesc(plannerContext, sourceDescs);
        }
        if (inputs.size() == 1) {
          return sourceDescs.get(0);
        }
        throw DruidException.defensive("Unable to create SourceDesc for Operator [%s]", scan);
      }

      /**
       * Extends the the current partial query with the new parent if possible.
       */
      @Override
      public Optional<Vertex> extendWith(Stack<DruidLogicalNode> stack, boolean isRoot)
      {
        Optional<PartialDruidQuery> newPartialQuery = extendPartialDruidQuery(stack, isRoot);
        if (!newPartialQuery.isPresent()) {
          return Optional.empty();
        }
        return Optional.of(createVertex(newPartialQuery.get(), inputs));
      }

      /**
       * Merges the given {@link RelNode} into the current {@link PartialDruidQuery}.
       */
      private Optional<PartialDruidQuery> extendPartialDruidQuery(Stack<DruidLogicalNode> stack, boolean isRoot)
      {
        DruidLogicalNode parentNode = stack.peek();
        if (accepts(stack, Stage.WHERE_FILTER, Filter.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter((Filter) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SELECT_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSelectProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.AGGREGATE, Aggregate.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregate((Aggregate) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.AGGREGATE_PROJECT, Project.class) ) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregateProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.HAVING_FILTER, Filter.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter((Filter) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SORT, Sort.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSort((Sort) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.SORT_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withSortProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.WINDOW, Window.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWindow((Window) parentNode);
          return Optional.of(newPartialQuery);
        }
        if (accepts(stack, Stage.WINDOW_PROJECT, Project.class)) {
          PartialDruidQuery newPartialQuery = partialDruidQuery.withWindowProject((Project) parentNode);
          return Optional.of(newPartialQuery);
        }
        return Optional.empty();
      }

      private boolean accepts(Stack<DruidLogicalNode> stack, Stage stage, Class<? extends RelNode> class1)
      {
        if (Project.class == class1 && stack.size() >= 2) {
          DruidLogicalNode parent2 = stack.get(stack.size() - 2);
          if (stage.ordinal() > stage.AGGREGATE.ordinal()) {
            if (parent2 instanceof DruidAggregate && !partialDruidQuery.canAccept(Stage.AGGREGATE)) {
              return false;
            }
          }
          if (stage.ordinal() > stage.SORT.ordinal()) {
            if (parent2 instanceof DruidSort && !partialDruidQuery.canAccept(Stage.SORT)) {
              return false;
            }
          }
        }

        return partialDruidQuery.canAccept(stage) && class1.isInstance(stack.peek());
      }

      @Override
      public SourceDesc unwrapSourceDesc()
      {
        if (canUnwrapSourceDesc()) {
          DruidQuery q = buildQuery(false);
          SourceDesc origInput = getSource();
          return new SourceDesc(origInput.dataSource, q.getOutputRowSignature());
        }
        throw DruidException.defensive("Can't unwrap source of vertex[%s]", partialDruidQuery);
      }

      @Override
      public boolean canUnwrapSourceDesc()
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
