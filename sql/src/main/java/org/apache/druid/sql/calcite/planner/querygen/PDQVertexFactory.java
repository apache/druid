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
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery.Stage;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link PartialDruidQuery} based {@link Vertex} factory.
 */
public class PDQVertexFactory
{
  private final PlannerContext plannerContext;
  private final RexBuilder rexBuilder;

  public PDQVertexFactory(PlannerContext plannerContext, RexBuilder rexBuilder)
  {
    this.plannerContext = plannerContext;
    this.rexBuilder = rexBuilder;
  }

  // FIXME
  public PDQVertex createVertex(RelNode scan)
  {
    PDQVertex vertex = new PDQVertex();
    vertex.partialDruidQuery = PartialDruidQuery.create(scan);
    return vertex;
  }

  // FIXME
  Vertex createVertex(RelNode node, Vertex inputVertex)
  {
    PDQVertex vertex = new PDQVertex();
    vertex.inputs = ImmutableList.of(inputVertex);
    vertex.partialDruidQuery = PartialDruidQuery.createOuterQuery(((PDQVertex) inputVertex).partialDruidQuery);
    return vertex;
  }

  Vertex createVertex(PartialDruidQuery partialDruidQuery, List<Vertex> inputs)
  {
    PDQVertex vertex = new PDQVertex();
    vertex.inputs = inputs;
    vertex.partialDruidQuery = partialDruidQuery;
    return vertex;
  }

  public class PDQVertex implements Vertex
  {
    PartialDruidQuery partialDruidQuery;
    List<Vertex> inputs;
    DruidTable queryTable;

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

    private InputDesc getInput()
    {
      if (partialDruidQuery.getScan() instanceof XInputProducer) {
        XInputProducer xInputProducer = (XInputProducer) partialDruidQuery.getScan();
        return xInputProducer.getInputDesc();
      }
      if (inputs.size() == 1) {
        DruidQuery inputQuery = inputs.get(0).buildQuery(false);
        return new InputDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());
      }
      if (partialDruidQuery.getScan() instanceof Union) {
        List<DataSource> dataSources = new ArrayList<>();
        RowSignature signature = null;
        for (Vertex inputVertex : inputs) {
          InputDesc unwrapInputDesc = inputVertex.unwrapInputDesc();
          dataSources.add(unwrapInputDesc.dataSource);

          if (signature == null) {
            signature = unwrapInputDesc.rowSignature;
          } else {
            if (!signature.equals(unwrapInputDesc.rowSignature)) {
              throw DruidException.defensive("Row signature mismatch FIXME");
            }
          }
        }
        return new InputDesc(new UnionDataSource(dataSources), signature);
      }
      throw new IllegalStateException();
    }

    private PDQVertex newVertex(PartialDruidQuery partialDruidQuery)
    {
      PDQVertex vertex = new PDQVertex();
      vertex.inputs = inputs;
      vertex.queryTable = queryTable;
      vertex.partialDruidQuery = partialDruidQuery;
      return vertex;
    }

    /**
     * Merges the given {@link RelNode} into the current partial query.
     *
     * @return the new merged vertex - or null if its not possible to merge
     */
    public Vertex mergeIntoDruidQuery(RelNode node, boolean isRoot)
    {
      if (accepts(node, Stage.WHERE_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SELECT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSelectProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.AGGREGATE, Aggregate.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregate((Aggregate) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.AGGREGATE_PROJECT, Project.class) && isRoot) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregateProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.HAVING_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SORT, Sort.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSort((Sort) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SORT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSortProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.WINDOW, Window.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindow((Window) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.WINDOW_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindowProject((Project) node);
        return newVertex(newPartialQuery);
      }
      return null;
    }

    private boolean accepts(RelNode node, Stage whereFilter, Class<? extends RelNode> class1)
    {
      return partialDruidQuery.canAccept(whereFilter) && class1.isInstance(node);
    }

    /**
     * Unwraps the input of this vertex - if it doesn't do anything beyond
     * reading its input.
     *
     * @throws DruidException
     *           if unwrap is not possible.
     */
    public InputDesc unwrapInputDesc()
    {
      if (canUnwrapInput()) {
        DruidQuery q = buildQuery(false);
        InputDesc origInput = getInput();
        return new InputDesc(origInput.dataSource, q.getOutputRowSignature());
      }
      throw DruidException.defensive("Can't unwrap input of vertex[%s]", partialDruidQuery);
    }

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

  // FIXME ok?
  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

}