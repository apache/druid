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

package org.apache.druid.sql.calcite.aggregation.builtin;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.GroupingAggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GroupingSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.GROUPING;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      VirtualColumnRegistry virtualColumnRegistry,
      String name,
      AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    List<String> arguments = aggregateCall.getArgList()
                                          .stream()
                                          .map(i -> getColumnName(
                                              plannerContext,
                                              inputAccessor.getInputRowSignature(),
                                              inputAccessor.getProject(),
                                              virtualColumnRegistry,
                                              inputAccessor.getRexBuilder().getTypeFactory(),
                                              i
                                          ))
                                          .filter(Objects::nonNull)
                                          .collect(Collectors.toList());

    if (arguments.size() < aggregateCall.getArgList().size()) {
      return null;
    }

    for (Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (!(factory instanceof GroupingAggregatorFactory)) {
          continue;
        }
        GroupingAggregatorFactory groupingFactory = (GroupingAggregatorFactory) factory;
        if (groupingFactory.getGroupings().equals(arguments)
            && groupingFactory.getName().equals(name)) {
          return Aggregation.create(groupingFactory);
        }
      }
    }
    AggregatorFactory factory = new GroupingAggregatorFactory(name, arguments);
    return Aggregation.create(factory);
  }

  @Nullable
  private String getColumnName(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      Project project,
      VirtualColumnRegistry virtualColumnRegistry,
      RelDataTypeFactory typeFactory,
      int fieldNumber
  )
  {
    RexNode node = Expressions.fromFieldAccess(typeFactory, rowSignature, project, fieldNumber);
    if (null == node) {
      return null;
    }
    DruidExpression expression = Expressions.toDruidExpression(plannerContext, rowSignature, node);
    if (null == expression) {
      return null;
    }
    if (expression.isDirectColumnAccess()) {
      return expression.getDirectColumn();
    }

    if (expression.isSimpleExtraction()) {
      return expression.getSimpleExtraction().getColumn();
    }

    return virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
        expression,
        node.getType()
    );
  }
}
