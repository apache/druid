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

package org.apache.druid.sql.calcite.aggregation;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.ExpressionTestHelper;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregationsTest extends CalciteTestBase
{
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("s", ColumnType.STRING)
                                                                .add("d", ColumnType.DOUBLE)
                                                                .build();

  private final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  @Test
  public void testNumericAggregatorWithNonNumericStringLiteral()
  {
    for (String value : new String[]{"m1", "abc", "", " 1 "}) {
      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> toDruidExpression(rexBuilder.makeLiteral(value))
      );
      Assertions.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
      Assertions.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
      Assertions.assertEquals(
          "Aggregator input must be numeric, got string literal ['" + value + "']."
          + " If you meant to refer to a column, use double quotes.",
          e.getMessage()
      );
    }
  }

  @Test
  public void testNumericAggregatorWithNumericStringLiteral()
  {
    // Strings that are implicitly converted to numbers at runtime, see ExprEval.computeNumber
    for (String value : new String[]{"1", "-1.5", "1e3", "true", "false"}) {
      final DruidExpression expression = toDruidExpression(rexBuilder.makeLiteral(value));
      Assertions.assertNotNull(expression, value);
      Assertions.assertFalse(expression.isDirectColumnAccess(), value);
    }
  }

  @Test
  public void testNumericAggregatorWithNullLiteral()
  {
    Assertions.assertNotNull(toDruidExpression(rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR))));
    Assertions.assertNotNull(toDruidExpression(rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.DOUBLE))));
    Assertions.assertNotNull(toDruidExpression(rexBuilder.constantNull()));
  }

  @Test
  public void testNumericAggregatorWithStringColumn()
  {
    // STRING columns are implicitly converted, so they are used through an expression instead of a direct access
    final DruidExpression expression = toDruidExpression(makeInputRef("s"));
    Assertions.assertNotNull(expression);
    Assertions.assertFalse(expression.isDirectColumnAccess());
    Assertions.assertEquals("\"s\"", expression.getExpression());
  }

  @Test
  public void testNumericAggregatorWithNumericColumn()
  {
    final DruidExpression expression = toDruidExpression(makeInputRef("d"));
    Assertions.assertNotNull(expression);
    Assertions.assertTrue(expression.isDirectColumnAccess());
    Assertions.assertEquals("d", expression.getDirectColumn());
  }

  private DruidExpression toDruidExpression(final RexNode rexNode)
  {
    return Aggregations.toDruidExpressionForNumericAggregator(
        ExpressionTestHelper.PLANNER_CONTEXT,
        ROW_SIGNATURE,
        rexNode
    );
  }

  private RexNode makeInputRef(final String columnName)
  {
    final RelDataType rowType = RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
    final int index = ROW_SIGNATURE.indexOf(columnName);
    return rexBuilder.makeInputRef(rowType.getFieldList().get(index).getType(), index);
  }
}
