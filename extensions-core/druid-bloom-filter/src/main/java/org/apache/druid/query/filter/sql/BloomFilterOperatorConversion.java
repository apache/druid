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

package org.apache.druid.query.filter.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.expressions.BloomFilterExprMacro;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class BloomFilterOperatorConversion extends DirectOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(BloomFilterExprMacro.FN_NAME))
      .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)
      .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
      .build();

  public BloomFilterOperatorConversion()
  {
    super(SQL_FUNCTION, BloomFilterExprMacro.FN_NAME);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );
    if (druidExpression == null) {
      return null;
    }

    String base64EncodedBloomKFilter = RexLiteral.stringValue(operands.get(1));
    final byte[] decoded = StringUtils.decodeBase64String(base64EncodedBloomKFilter);
    BloomKFilter filter;
    BloomKFilterHolder holder;
    try {
      filter = BloomFilterSerializersModule.bloomKFilterFromBytes(decoded);
      holder = BloomKFilterHolder.fromBloomKFilter(filter);
    }
    catch (IOException ioe) {
      throw new RuntimeException("Failed to deserialize bloom filter", ioe);
    }

    if (druidExpression.isSimpleExtraction()) {
      return new BloomDimFilter(
          druidExpression.getSimpleExtraction().getColumn(),
          holder,
          druidExpression.getSimpleExtraction().getExtractionFn(),
          null
      );
    } else if (virtualColumnRegistry != null) {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          druidExpression,
          operands.get(0).getType().getSqlTypeName()
      );
      if (virtualColumn == null) {
        return null;
      }
      return new BloomDimFilter(
          virtualColumn.getOutputName(),
          holder,
          null,
          null
      );
    } else {
      return null;
    }
  }
}
