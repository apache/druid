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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.convertlet.DruidConvertletFactory;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DruidNVLOperatorConversion extends CoalesceOperatorConversion
    implements DruidConvertletFactory, SqlRexConvertlet
{
  private static final SqlFunction OPERATOR = new SqlNVLFunction();

  public static class SqlNVLFunction extends SqlFunction
  {

    public SqlNVLFunction()
    {
      super(
          "NVL",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.LEAST_RESTRICTIVE
              .andThen(SqlTypeTransforms.TO_NULLABLE_ALL),
          null,
          OperandTypes.SAME_SAME,
          SqlFunctionCategory.SYSTEM
      );
    }
  }

  public static final DruidNVLOperatorConversion INSTANCE = new DruidNVLOperatorConversion();

  public DruidNVLOperatorConversion()
  {
    super(OPERATOR);
  }

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return this;
  }

  @Override
  public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
  {

    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<RexNode> newArgs = new ArrayList<RexNode>();
    for (SqlNode node : call.getOperandList()) {
      newArgs.add(cx.convertExpression(node));
    }
    return rexBuilder.makeCall(OPERATOR, newArgs);
  }

  @Override
  public List<SqlOperator> operators()
  {
    return ImmutableList.of(calciteOperator());
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode)
  {
    if (true) {
      throw new RuntimeException();
    }
    return null;
  }

}

