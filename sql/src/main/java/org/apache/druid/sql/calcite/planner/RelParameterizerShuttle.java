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

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;

/**
 * Traverse {@link RelNode} tree and replaces all {@link RexDynamicParam} with {@link org.apache.calcite.rex.RexLiteral}
 * using {@link RexBuilder} if a value binding exists for the parameter. All parameters must have a value by the time
 * {@link RelParameterizerShuttle} is run, or else it will throw an exception.
 *
 * Note: none of the tests currently hit this anymore since {@link SqlParameterizerShuttle} has been modified to handle
 * most common jdbc types, but leaving this here provides a safety net to try again to convert parameters
 * to literal values in case {@link SqlParameterizerShuttle} fails.
 */
public class RelParameterizerShuttle implements RelShuttle
{
  private final PlannerContext plannerContext;

  public RelParameterizerShuttle(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public RelNode visit(TableScan scan)
  {
    return bindRel(scan);
  }

  @Override
  public RelNode visit(TableFunctionScan scan)
  {
    return bindRel(scan);
  }

  @Override
  public RelNode visit(LogicalValues values)
  {
    return bindRel(values);
  }

  @Override
  public RelNode visit(LogicalFilter filter)
  {
    return bindRel(filter);
  }

  @Override
  public RelNode visit(LogicalProject project)
  {
    return bindRel(project);
  }

  @Override
  public RelNode visit(LogicalJoin join)
  {
    return bindRel(join);
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate)
  {
    return bindRel(correlate);
  }

  @Override
  public RelNode visit(LogicalUnion union)
  {
    return bindRel(union);
  }

  @Override
  public RelNode visit(LogicalIntersect intersect)
  {
    return bindRel(intersect);
  }

  @Override
  public RelNode visit(LogicalMinus minus)
  {
    return bindRel(minus);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate)
  {
    return bindRel(aggregate);
  }

  @Override
  public RelNode visit(LogicalMatch match)
  {
    return bindRel(match);
  }

  @Override
  public RelNode visit(LogicalSort sort)
  {
    final RexBuilder builder = sort.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = sort.getCluster().getTypeFactory();
    RexNode newFetch = bind(sort.fetch, builder, typeFactory);
    RexNode newOffset = bind(sort.offset, builder, typeFactory);
    sort = (LogicalSort) sort.copy(sort.getTraitSet(), sort.getInput(), sort.getCollation(), newOffset, newFetch);
    return bindRel(sort, builder, typeFactory);
  }

  @Override
  public RelNode visit(LogicalExchange exchange)
  {
    return bindRel(exchange);
  }

  @Override
  public RelNode visit(RelNode other)
  {
    return bindRel(other);
  }

  private RelNode bindRel(RelNode node)
  {
    final RexBuilder builder = node.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = node.getCluster().getTypeFactory();
    return bindRel(node, builder, typeFactory);
  }

  private RelNode bindRel(RelNode node, RexBuilder builder, RelDataTypeFactory typeFactory)
  {
    final RexShuttle binder = new RexShuttle()
    {
      @Override
      public RexNode visitDynamicParam(RexDynamicParam dynamicParam)
      {
        return bind(dynamicParam, builder, typeFactory);
      }
    };
    node = node.accept(binder);
    node.childrenAccept(new RelVisitor()
    {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent)
      {
        super.visit(node, ordinal, parent);
        RelNode transformed = node.accept(binder);
        if (!node.equals(transformed)) {
          parent.replaceInput(ordinal, transformed);
        }
      }
    });
    return node;
  }

  private RexNode bind(RexNode node, RexBuilder builder, RelDataTypeFactory typeFactory)
  {
    if (node instanceof RexDynamicParam) {
      RexDynamicParam dynamicParam = (RexDynamicParam) node;
      // if we have a value for dynamic parameter, replace with a literal, else add to list of unbound parameters
      if (plannerContext.getParameters().size() > dynamicParam.getIndex()) {
        TypedValue param = plannerContext.getParameters().get(dynamicParam.getIndex());
        if (param.value == null) {
          return builder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        }
        SqlTypeName typeName = SqlTypeName.getNameForJdbcType(param.type.typeId);
        return builder.makeLiteral(
            param.value,
            typeFactory.createSqlType(typeName),
            true
        );
      } else {
        throw new ISE("Parameter: [%s] is not bound", dynamicParam.getName());
      }
    }
    return node;
  }
}
