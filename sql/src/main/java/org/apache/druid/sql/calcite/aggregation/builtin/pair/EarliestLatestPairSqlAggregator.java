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

package org.apache.druid.sql.calcite.aggregation.builtin.pair;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EarliestLatestPairSqlAggregator implements SqlAggregator
{
  public static final SqlAggregator[] DEFINED_AGGS = new SqlAggregator[]{
      new EarliestLatestPairSqlAggregator(AggregatorType.EARLIEST, PayloadType.STRING),
      new EarliestLatestPairSqlAggregator(AggregatorType.LATEST, PayloadType.STRING)
  };

  enum AggregatorType
  {
    EARLIEST, LATEST
  }

  enum PayloadType
  {
    STRING, LONG, DOUBLE
  }


  private final AggregatorType aggregatorType;
  private final PayloadType payloadType;
  private final SqlAggFunction function;

  private EarliestLatestPairSqlAggregator(AggregatorType aggregatorType, PayloadType payloadType)
  {
    this.aggregatorType = aggregatorType;
    this.payloadType = payloadType;

    String name = StringUtils.format("%s_%s", payloadType.name(), aggregatorType.name());
    this.function = new SqlAggFunction(
        name,
        null,
        SqlKind.OTHER_FUNCTION,
        opBinding -> RowSignatures.makeComplexType(
            opBinding.getTypeFactory(),
            StringLastAggregatorFactory.TYPE,
            true
        ),
        InferTypes.RETURN_TYPE,
        OperandTypes.or(
            RowSignatures.complexTypeChecker(StringLastAggregatorFactory.TYPE),
            OperandTypes.sequence(
                "'" + name + "(pairColumn, maxBytesPerString)'\n",
                RowSignatures.complexTypeChecker(StringLastAggregatorFactory.TYPE),
                OperandTypes.and(OperandTypes.NUMERIC, OperandTypes.LITERAL)
            ),
            OperandTypes.sequence(
                "'" + name + "(versionColumn, expr)'\n",
                OperandTypes.or(OperandTypes.family(SqlTypeFamily.INTEGER), OperandTypes.family(SqlTypeFamily.TIMESTAMP)),
                OperandTypes.ANY
            ),
            OperandTypes.sequence(
                "'" + name + "(versionColumn, expr, maxBytesPerString)'\n",
                OperandTypes.or(OperandTypes.family(SqlTypeFamily.INTEGER), OperandTypes.family(SqlTypeFamily.TIMESTAMP)),
                OperandTypes.ANY,
                OperandTypes.and(OperandTypes.NUMERIC, OperandTypes.LITERAL)
            )
        ),
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    )
    {
    };
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
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final List<RexNode> rexNodes = aggregateCall
        .getArgList()
        .stream()
        .map(i -> Expressions.fromFieldAccess(rexBuilder.getTypeFactory(), rowSignature, project, i))
        .collect(Collectors.toList());

    final List<DruidExpression> args = Expressions.toDruidExpressions(plannerContext, rowSignature, rexNodes);

    if (args == null) {
      return null;
    }

    final ColumnType outputType = Calcites.getColumnTypeForRelDataType(aggregateCall.getType());
    if (outputType == null) {
      throw new ISE(
          "Cannot translate output sqlTypeName[%s] to Druid type for aggregator[%s]",
          aggregateCall.getType().getSqlTypeName(),
          aggregateCall.getName()
      );
    }

    String fieldName = null;
    String versionColumnName = getColumnName(virtualColumnRegistry, args.get(0), rexNodes.get(0));
    String foldColumnName = null;
    Integer maxStringBytes = null;
    switch (args.size()) {
      case 1:
        foldColumnName = versionColumnName;
        versionColumnName = null;
        break;
      case 2:
        final RexNode secondArg = rexNodes.get(1);
        if (secondArg.isA(SqlKind.LITERAL)) {
          foldColumnName = versionColumnName;
          versionColumnName = null;
          maxStringBytes = RexLiteral.intValue(secondArg);
        } else {
          fieldName = getColumnName(virtualColumnRegistry, args.get(1), secondArg);
        }
        break;
      case 3:
        fieldName = getColumnName(virtualColumnRegistry, args.get(1), rexNodes.get(1));
        maxStringBytes = RexLiteral.intValue(rexNodes.get(2));
        break;
    }

    final AggregatorFactory theAggFactory;
    switch (aggregatorType) {
      case EARLIEST:
        switch (payloadType) {
          case STRING:
            theAggFactory =
                new StringFirstAggregatorFactory(name, fieldName, versionColumnName, foldColumnName, maxStringBytes, false);
            break;
          default:
            throw new UOE("payload type [%s] is unsupported", payloadType);
        }
        break;
      case LATEST:
        switch (payloadType) {
          case STRING:
            theAggFactory =
                new StringLastAggregatorFactory(name, fieldName, versionColumnName, foldColumnName, maxStringBytes, false);
            break;
          default:
            throw new UOE("payload type [%s] is unsupported", payloadType);
        }
        break;
      default:
        throw new UOE("aggregator type [%s] is unsupported", aggregatorType);
    }

    return Aggregation.create(Collections.singletonList(theAggFactory), null);
  }

  static String getColumnName(
      VirtualColumnRegistry virtualColumnRegistry,
      DruidExpression arg,
      RexNode rexNode
  )
  {
    String columnName;
    if (arg.isDirectColumnAccess()) {
      columnName = arg.getDirectColumn();
    } else {
      final RelDataType dataType = rexNode.getType();
      columnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, dataType);
    }
    return columnName;
  }
}
