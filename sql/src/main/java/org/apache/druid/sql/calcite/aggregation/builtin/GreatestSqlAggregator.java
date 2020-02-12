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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.DoubleGreatestPostAggregator;
import org.apache.druid.query.aggregation.post.LongGreatestPostAggregator;
import org.apache.druid.segment.column.ValueType;

import java.util.List;

/**
 * Calcite integration class for Greatest post aggregators of Long & Double types.
 * It applies Max aggregators over the provided fields/expressions & combines their results via Field access post aggregators.
 */
public class GreatestSqlAggregator extends MultiColumnSqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new GreatestSqlAggFunction();
  private static final String NAME = "GREATEST";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  AggregatorFactory createAggregatorFactory(
      ValueType valueType,
      String prefixedName,
      FieldInfo fieldInfo,
      ExprMacroTable macroTable
  )
  {
    final AggregatorFactory aggregatorFactory;
    switch (valueType) {
      case LONG:
        aggregatorFactory = new LongMaxAggregatorFactory(prefixedName, fieldInfo.fieldName, fieldInfo.expression, macroTable);
        break;
      case FLOAT:
      case DOUBLE:
        aggregatorFactory = new DoubleMaxAggregatorFactory(prefixedName, fieldInfo.fieldName, fieldInfo.expression, macroTable);
        break;
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", valueType);
    }
    return aggregatorFactory;
  }

  @Override
  PostAggregator createFinalPostAggregator(
      ValueType valueType,
      String name,
      List<PostAggregator> postAggregators
  )
  {
    final PostAggregator finalPostAggregator;
    switch (valueType) {
      case LONG:
        finalPostAggregator = new LongGreatestPostAggregator(name, postAggregators);
        break;
      case FLOAT:
      case DOUBLE:
        finalPostAggregator = new DoubleGreatestPostAggregator(name, postAggregators);
        break;
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", valueType);
    }
    return finalPostAggregator;
  }

  /**
   * Calcite SQL function definition
   */
  private static class GreatestSqlAggFunction extends SqlAggFunction
  {
    GreatestSqlAggFunction()
    {
      /*
       * The constructor params are explained as follows,
       * name: SQL function name
       * sqlIdentifier: null for built-in functions
       * kind: SqlKind.GREATEST
       * returnTypeInference: biggest operand type & nullable if any of the operands is nullable
       * operandTypeInference: same as return type
       * operandTypeChecker: variadic function with at least one argument
       * funcType: System
       * requiresOrder: No
       * requiresOver: No
       * requiresGroupOrder: Not allowed
       */
      super(
          NAME,
          null,
          SqlKind.GREATEST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE, SqlTypeTransforms.TO_NULLABLE),
          InferTypes.RETURN_TYPE,
          OperandTypes.ONE_OR_MORE,
          SqlFunctionCategory.SYSTEM,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
