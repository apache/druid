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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

/**
 * Operator that tests whether its left operand is included in the range of
 * values covered by search arguments.
 * This is the Druid version support for https://issues.apache.org/jira/browse/CALCITE-4173
 *
 * SEARCH operator tests whether an operand belongs to the range set.
 * A RexCall to SEARCH is converted back to SQL, typically an IN or OR.
 */

public class SearchOperatorConversion implements SqlOperatorConversion
{
  private static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl(DruidTypeSystem.INSTANCE));

  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.SEARCH;
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    return Expressions.toFilter(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        RexUtil.expandSearch(REX_BUILDER, null, rexNode)
    );
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        RexUtil.expandSearch(REX_BUILDER, null, rexNode)
    );
  }
}
