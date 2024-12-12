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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DefaultOperandTypeChecker;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class EarliestLatestBySqlAggregator implements SqlAggregator
{
  public static final SqlAggregator EARLIEST_BY = new EarliestLatestBySqlAggregator(EarliestLatestAnySqlAggregator.AggregatorType.EARLIEST);
  public static final SqlAggregator LATEST_BY = new EarliestLatestBySqlAggregator(EarliestLatestAnySqlAggregator.AggregatorType.LATEST);

  private final EarliestLatestAnySqlAggregator.AggregatorType aggregatorType;
  private final SqlAggFunction function;

  private EarliestLatestBySqlAggregator(final EarliestLatestAnySqlAggregator.AggregatorType aggregatorType)
  {
    this.aggregatorType = aggregatorType;
    this.function = new EarliestByLatestBySqlAggFunction(aggregatorType);
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return function;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final List<RexNode> rexNodes = inputAccessor.getFields(aggregateCall.getArgList());

    final List<DruidExpression> args = Expressions.toDruidExpressions(plannerContext, inputAccessor.getInputRowSignature(), rexNodes);

    if (args == null) {
      return null;
    }

    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;
    final ColumnType outputType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (outputType == null) {
      throw new ISE(
          "Cannot translate output sqlTypeName[%s] to Druid type for aggregator[%s]",
          aggregateCall.getType().getSqlTypeName(),
          aggregateCall.getName()
      );
    }

    final String fieldName = EarliestLatestAnySqlAggregator.getColumnName(
        virtualColumnRegistry,
        args.get(0),
        rexNodes.get(0)
    );

    final AggregatorFactory theAggFactory;
    switch (args.size()) {
      case 2:
        theAggFactory = aggregatorType.createAggregatorFactory(
            aggregatorName,
            fieldName,
            EarliestLatestAnySqlAggregator.getColumnName(
                virtualColumnRegistry,
                args.get(1),
                rexNodes.get(1)
            ),
            outputType,
            null,
            true
        );
        break;
      case 3:
        int maxStringBytes;
        try {
          maxStringBytes = RexLiteral.intValue(rexNodes.get(2));
        }
        catch (AssertionError ae) {
          plannerContext.setPlanningError(
              "The third argument '%s' to function '%s' is not a number",
              rexNodes.get(2),
              aggregateCall.getName()
          );
          return null;
        }
        theAggFactory = aggregatorType.createAggregatorFactory(
            aggregatorName,
            fieldName,
            EarliestLatestAnySqlAggregator.getColumnName(
                virtualColumnRegistry,
                args.get(1),
                rexNodes.get(1)
            ),
            outputType,
            maxStringBytes,
            true
        );
        break;
      default:
        throw new IAE(
            "aggregation[%s], Invalid number of arguments[%,d] to [%s] operator",
            aggregatorName,
            args.size(),
            aggregatorType.name()
        );
    }

    return Aggregation.create(
        Collections.singletonList(theAggFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(name, aggregatorName) : null
    );
  }

  private static class EarliestByLatestBySqlAggFunction extends SqlAggFunction
  {
    private static final SqlReturnTypeInference EARLIEST_LATEST_ARG0_RETURN_TYPE_INFERENCE =
        new EarliestLatestAnySqlAggregator.EarliestLatestReturnTypeInference(0);

    EarliestByLatestBySqlAggFunction(EarliestLatestAnySqlAggregator.AggregatorType aggregatorType)
    {
      super(
          StringUtils.format("%s_BY", aggregatorType.name()),
          null,
          SqlKind.OTHER_FUNCTION,
          EARLIEST_LATEST_ARG0_RETURN_TYPE_INFERENCE,
          InferTypes.RETURN_TYPE,
          DefaultOperandTypeChecker
              .builder()
              .operandNames("expr", "timeColumn", "maxBytesPerString")
              .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.NUMERIC)
              .requiredOperandCount(2)
              .literalOperands(2)
              .build(),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
