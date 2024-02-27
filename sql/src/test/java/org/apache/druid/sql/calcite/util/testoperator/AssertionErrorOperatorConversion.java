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

package org.apache.druid.sql.calcite.util.testoperator;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.http.SqlResourceTest;

import javax.annotation.Nullable;

/**
 * There are various points where Calcite feels it is acceptable to throw an AssertionError when it receives bad
 * input. This is unfortunate as a java.lang.Error is very clearly documented to be something that nobody should
 * try to catch. But, we can editorialize all we want, we still have to deal with it. So, this operator triggers
 * the AssertionError behavior by using RexLiteral.intValue with bad input (a RexNode that is not a literal).
 *
 * The test {@link SqlResourceTest#testAssertionErrorThrowsErrorWithFilterResponse()} verifies that our exception
 * handling deals with this meaningfully.
 */
public class AssertionErrorOperatorConversion implements SqlOperatorConversion
{
  private static final SqlOperator OPERATOR =
      OperatorConversions.operatorBuilder("assertion_error")
                         .operandTypes()
                         .returnTypeNonNull(SqlTypeName.BIGINT)
                         .build();

  @Override
  public SqlOperator calciteOperator()
  {
    return OPERATOR;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    // Throws AssertionError. See class-level javadoc for rationale about why we're doing this.
    RexLiteral.intValue(rexNode);
    return null;
  }
}
