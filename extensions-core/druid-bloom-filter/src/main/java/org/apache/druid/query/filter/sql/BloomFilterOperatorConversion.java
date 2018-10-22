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
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.hive.common.util.BloomKFilter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class BloomFilterOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("BLOOM_FILTER_TEST")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
      .build();

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );
    if (druidExpression == null || !druidExpression.isSimpleExtraction()) {
      return null;
    }

    String base64EncodedBloomKFilter = RexLiteral.stringValue(operands.get(1));
    byte[] bytes = StringUtils.toUtf8(base64EncodedBloomKFilter);
    byte[] decoded = Base64.decodeBase64(bytes);
    try {
      BloomKFilter filter = BloomFilterSerializersModule.bloomKFilterFromBytes(decoded);

      return new BloomDimFilter(
          druidExpression.getSimpleExtraction().getColumn(),
          BloomKFilterHolder.fromBloomKFilter(filter),
          druidExpression.getSimpleExtraction().getExtractionFn()
      );

    }
    catch (IOException ioe) {
      return null;
    }
  }
}
