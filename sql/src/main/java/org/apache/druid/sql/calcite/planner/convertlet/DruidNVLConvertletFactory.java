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

package org.apache.druid.sql.calcite.planner.convertlet;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.sql.calcite.expression.builtin.DruidNVLOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.List;

public class DruidNVLConvertletFactory implements DruidConvertletFactory, SqlRexConvertlet
{
  public static final DruidNVLConvertletFactory INSTANCE = new DruidNVLConvertletFactory();
  private static final SqlFunction OPERATOR = new DruidNVLOperatorConversion.SqlNVLFunction();

  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call)
  {
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<RexNode> newArgs = new ArrayList<RexNode>();
    for (SqlNode node : call.getOperandList()) {
      newArgs.add(cx.convertExpression(node));
    }
    return rexBuilder.makeCall(OPERATOR, newArgs);
  }

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return this;
  }

  @Override
  public List<SqlOperator> operators()
  {
    return ImmutableList.of(OPERATOR);
  }
}
