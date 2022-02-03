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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EarliestLatestAnySqlAggregator implements SqlAggregator
{
  public static final SqlAggregator EARLIEST = new EarliestLatestAnySqlAggregator(AggregatorType.EARLIEST);
  public static final SqlAggregator LATEST = new EarliestLatestAnySqlAggregator(AggregatorType.LATEST);
  public static final SqlAggregator ANY_VALUE = new EarliestLatestAnySqlAggregator(AggregatorType.ANY_VALUE);

  enum AggregatorType
  {
    EARLIEST {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, String timeColumn, ColumnType type, int maxStringBytes)
      {
        switch (type.getType()) {
          case LONG:
            return new LongFirstAggregatorFactory(name, fieldName, timeColumn);
          case FLOAT:
            return new FloatFirstAggregatorFactory(name, fieldName, timeColumn);
          case DOUBLE:
            return new DoubleFirstAggregatorFactory(name, fieldName, timeColumn);
          case STRING:
          case COMPLEX:
            return new StringFirstAggregatorFactory(name, fieldName, timeColumn, maxStringBytes);
          default:
            throw new UnsupportedSQLQueryException("EARLIEST aggregator is not supported for '%s' type", type);
        }
      }
    },

    LATEST {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, String timeColumn, ColumnType type, int maxStringBytes)
      {
        switch (type.getType()) {
          case LONG:
            return new LongLastAggregatorFactory(name, fieldName, timeColumn);
          case FLOAT:
            return new FloatLastAggregatorFactory(name, fieldName, timeColumn);
          case DOUBLE:
            return new DoubleLastAggregatorFactory(name, fieldName, timeColumn);
          case STRING:
          case COMPLEX:
            return new StringLastAggregatorFactory(name, fieldName, timeColumn, maxStringBytes);
          default:
            throw new UnsupportedSQLQueryException("LATEST aggregator is not supported for '%s' type", type);
        }
      }
    },

    ANY_VALUE {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, String timeColumn, ColumnType type, int maxStringBytes)
      {
        switch (type.getType()) {
          case LONG:
            return new LongAnyAggregatorFactory(name, fieldName);
          case FLOAT:
            return new FloatAnyAggregatorFactory(name, fieldName);
          case DOUBLE:
            return new DoubleAnyAggregatorFactory(name, fieldName);
          case STRING:
            return new StringAnyAggregatorFactory(name, fieldName, maxStringBytes);
          default:
            throw new UnsupportedSQLQueryException("ANY aggregation is not supported for '%s' type", type);
        }
      }
    };

    abstract AggregatorFactory createAggregatorFactory(
        String name,
        String fieldName,
        String timeColumn,
        ColumnType outputType,
        int maxStringBytes
    );
  }

  private final AggregatorType aggregatorType;
  private final SqlAggFunction function;

  private EarliestLatestAnySqlAggregator(final AggregatorType aggregatorType)
  {
    this.aggregatorType = aggregatorType;
    this.function = new EarliestLatestSqlAggFunction(aggregatorType);
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
        .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
        .collect(Collectors.toList());

    final List<DruidExpression> args = Expressions.toDruidExpressions(plannerContext, rowSignature, rexNodes);

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

    final String fieldName = getColumnName(plannerContext, virtualColumnRegistry, args.get(0), rexNodes.get(0));

    final AggregatorFactory theAggFactory;
    switch (args.size()) {
      case 1:
        theAggFactory = aggregatorType.createAggregatorFactory(aggregatorName, fieldName, null, outputType, -1);
        break;
      case 2:
        theAggFactory = aggregatorType.createAggregatorFactory(
            aggregatorName,
            fieldName,
            null,
            outputType,
            RexLiteral.intValue(rexNodes.get(1))
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

  static String getColumnName(
      PlannerContext plannerContext,
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
      final VirtualColumn virtualColumn =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, arg, dataType);
      columnName = virtualColumn.getOutputName();
    }
    return columnName;
  }

  static class EarliestLatestReturnTypeInference implements SqlReturnTypeInference
  {
    private final int ordinal;

    public EarliestLatestReturnTypeInference(int ordinal)
    {
      this.ordinal = ordinal;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding)
    {
      RelDataType type = sqlOperatorBinding.getOperandType(this.ordinal);
      // For non-number and non-string type, which is COMPLEX type, we set the return type to VARCHAR.
      if (!SqlTypeUtil.isNumeric(type) &&
          !SqlTypeUtil.isString(type)) {
        return sqlOperatorBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
      } else {
        return type;
      }
    }
  }

  private static class EarliestLatestSqlAggFunction extends SqlAggFunction
  {
    private static final EarliestLatestReturnTypeInference EARLIEST_LATEST_ARG0_RETURN_TYPE_INFERENCE =
        new EarliestLatestReturnTypeInference(0);

    EarliestLatestSqlAggFunction(AggregatorType aggregatorType)
    {
      super(
          aggregatorType.name(),
          null,
          SqlKind.OTHER_FUNCTION,
          EARLIEST_LATEST_ARG0_RETURN_TYPE_INFERENCE,
          InferTypes.RETURN_TYPE,
          OperandTypes.or(
              OperandTypes.NUMERIC,
              OperandTypes.BOOLEAN,
              OperandTypes.sequence(
                  "'" + aggregatorType.name() + "(expr, maxBytesPerString)'\n",
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.NUMERIC, OperandTypes.LITERAL)
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
