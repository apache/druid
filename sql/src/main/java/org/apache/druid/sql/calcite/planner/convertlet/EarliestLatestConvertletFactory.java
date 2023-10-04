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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.sql.calcite.aggregation.builtin.EarliestLatestAnySqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.EarliestLatestBySqlAggregator;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.Collections;
import java.util.List;

public class EarliestLatestConvertletFactory implements DruidConvertletFactory
{
    public static final EarliestLatestConvertletFactory INSTANCE = new EarliestLatestConvertletFactory();

    private static final String NAME = "LATER";

    private static final SqlOperator OPERATOR = OperatorConversions
            .operatorBuilder(NAME)
            .operandTypeChecker(
                    OperandTypes.or(
                            OperandTypes.ANY,
                            OperandTypes.sequence(
                                    "'" + NAME + "(expr, maxBytesPerString)'",
                                    OperandTypes.ANY,
                                    OperandTypes.and(OperandTypes.NUMERIC, OperandTypes.LITERAL)
                            )
                    )
            )
            .returnTypeInference(new EarliestLatestAnySqlAggregator.EarliestLatestReturnTypeInference(0))
            .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .build();

    private EarliestLatestConvertletFactory()
    {
        // Singleton.
    }

    @Override
    public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
    {
        return new EarliestLatestConvertlet();
    }

    @Override
    public List<SqlOperator> operators()
    {
        return Collections.singletonList(OPERATOR);
    }

    private static class EarliestLatestConvertlet implements SqlRexConvertlet
    {
        @Override
        public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
        {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final RexNode columnOperand = cx.convertExpression(call.getOperandList().get(0));
            final RexNode timeColumnOperand = cx.convertExpression(new SqlIdentifier("__time", SqlParserPos.ZERO));

            return rexBuilder.makeCall(
                    EarliestLatestBySqlAggregator.LATEST_BY.calciteFunction(),
                    columnOperand, timeColumnOperand
            );
        }
    }
}