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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EarliestLatestAnySqlAggregator implements SqlAggregator
{
  public static final SqlAggregator EARLIEST = new EarliestLatestAnySqlAggregator(AggregatorType.EARLIEST);
  public static final SqlAggregator LATEST = new EarliestLatestAnySqlAggregator(AggregatorType.LATEST);
  public static final SqlAggregator ANY_VALUE = new EarliestLatestAnySqlAggregator(AggregatorType.ANY_VALUE);

  enum AggregatorType
  {
    EARLIEST {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, ValueType type, int maxStringBytes)
      {
        switch (type) {
          case LONG:
            return new LongFirstAggregatorFactory(name, fieldName);
          case FLOAT:
            return new FloatFirstAggregatorFactory(name, fieldName);
          case DOUBLE:
            return new DoubleFirstAggregatorFactory(name, fieldName);
          case STRING:
            return new StringFirstAggregatorFactory(name, fieldName, maxStringBytes);
          default:
            throw new ISE("Cannot build EARLIEST aggregatorFactory for type[%s]", type);
        }
      }
    },

    LATEST {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, ValueType type, int maxStringBytes)
      {
        switch (type) {
          case LONG:
            return new LongLastAggregatorFactory(name, fieldName);
          case FLOAT:
            return new FloatLastAggregatorFactory(name, fieldName);
          case DOUBLE:
            return new DoubleLastAggregatorFactory(name, fieldName);
          case STRING:
            return new StringLastAggregatorFactory(name, fieldName, maxStringBytes);
          default:
            throw new ISE("Cannot build LATEST aggregatorFactory for type[%s]", type);
        }
      }
    },

    ANY_VALUE {
      @Override
      AggregatorFactory createAggregatorFactory(String name, String fieldName, ValueType type, int maxStringBytes)
      {
        switch (type) {
          case LONG:
            return new LongAnyAggregatorFactory(name, fieldName);
          case FLOAT:
            return new FloatAnyAggregatorFactory(name, fieldName);
          case DOUBLE:
            return new DoubleAnyAggregatorFactory(name, fieldName);
          case STRING:
            return new StringAnyAggregatorFactory(name, fieldName, maxStringBytes);
          default:
            throw new ISE("Cannot build ANY aggregatorFactory for type[%s]", type);
        }
      }
    };

    abstract AggregatorFactory createAggregatorFactory(
        String name,
        String fieldName,
        ValueType outputType,
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
    final String fieldName;

    if (args.get(0).isDirectColumnAccess()) {
      fieldName = args.get(0).getDirectColumn();
    } else {
      final SqlTypeName sqlTypeName = rexNodes.get(0).getType().getSqlTypeName();
      final VirtualColumn virtualColumn =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, args.get(0), sqlTypeName);
      fieldName = virtualColumn.getOutputName();
    }

    // Second arg must be a literal, if it exists (the type signature below requires it).
    final int maxBytes = rexNodes.size() > 1 ? RexLiteral.intValue(rexNodes.get(1)) : -1;

    final ValueType outputType = Calcites.getValueTypeForSqlTypeName(aggregateCall.getType().getSqlTypeName());
    if (outputType == null) {
      throw new ISE(
          "Cannot translate output sqlTypeName[%s] to Druid type for aggregator[%s]",
          aggregateCall.getType().getSqlTypeName(),
          aggregateCall.getName()
      );
    }

    return Aggregation.create(
        Stream.of(virtualColumnRegistry.getVirtualColumn(fieldName))
              .filter(Objects::nonNull)
              .collect(Collectors.toList()),
        Collections.singletonList(
            aggregatorType.createAggregatorFactory(
                aggregatorName,
                fieldName,
                outputType,
                maxBytes
            )
        ),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(name, aggregatorName) : null
    );
  }

  private static class EarliestLatestSqlAggFunction extends SqlAggFunction
  {
    EarliestLatestSqlAggFunction(AggregatorType aggregatorType)
    {
      super(
          aggregatorType.name(),
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.or(
              OperandTypes.NUMERIC,
              OperandTypes.BOOLEAN,
              OperandTypes.sequence(
                  "'" + aggregatorType.name() + "(expr, maxBytesPerString)'\n",
                  OperandTypes.STRING,
                  OperandTypes.and(OperandTypes.NUMERIC, OperandTypes.LITERAL)
              )
          ),
          SqlFunctionCategory.STRING,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
