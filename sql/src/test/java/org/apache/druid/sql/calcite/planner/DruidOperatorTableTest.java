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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.rel.Windowing;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

public class DruidOperatorTableTest
{
  @Test
  public void testBuiltInOperatorTable()
  {
    DruidOperatorTable operatorTable = new DruidOperatorTable(ImmutableSet.of(), ImmutableSet.of());
    List<SqlOperator> operatorList = operatorTable.getOperatorList();
    Assert.assertNotNull(operatorList);
    Assert.assertTrue("Built-in operators should be loaded by default", operatorList.size() > 0);
  }

  @Test
  public void testIsFunctionSyntax()
  {
    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION));
    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION_STAR));
    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION_ID));
    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.SPECIAL));
    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.INTERNAL));

    Assert.assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.BINARY));
    Assert.assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.PREFIX));
    Assert.assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.POSTFIX));
  }

  @Test
  public void testCustomOperatorTable()
  {
    final SqlOperator operator1 = OperatorConversions
        .operatorBuilder("FOO")
        .operandTypes(SqlTypeFamily.ANY)
        .requiredOperandCount(0)
        .returnTypeInference(
            opBinding -> RowSignatures.makeComplexType(
                opBinding.getTypeFactory(),
                ColumnType.ofComplex("fooComplex"),
                true
            )
        )
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();

    final SqlOperator operator2 = SqlStdOperatorTable.PLUS;
    final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();

    extractionOperators.add(new DirectOperatorConversion(operator1, "foo_fn"));
    extractionOperators.add(new DirectOperatorConversion(operator2, "plus_is_not_a_fn"));

    DruidOperatorTable operatorTable = new DruidOperatorTable(ImmutableSet.of(), extractionOperators);
    List<SqlOperator> operatorList = operatorTable.getOperatorList();
    Assert.assertNotNull(operatorList);
    Assert.assertTrue("We should have at least two operators -- the ones we loaded above plus the built-in"
                      + " operators that gets loaded by default", operatorList.size() > 2);

    Assert.assertTrue(operatorList.contains(operator1));
    Assert.assertTrue(operatorList.contains(operator2));

    Assert.assertTrue(DruidOperatorTable.isFunctionSyntax(operator1.getSyntax()));
    Assert.assertFalse(DruidOperatorTable.isFunctionSyntax(operator2.getSyntax()));
  }

  @Test
  public void testBuiltinWindowOperatorsSupportFramingAsExpected()
  {
    DruidOperatorTable operatorTable = new DruidOperatorTable(ImmutableSet.of(), ImmutableSet.of());
    ImmutableSet<String> keySet = Windowing.KNOWN_WINDOW_FNS.keySet();
    Set<SqlOperator> windowOps = operatorTable.getOperatorList().stream()
        .filter(o -> keySet.contains(o.getKind().toString())).collect(Collectors.toSet());
    for (SqlOperator operator : windowOps) {
      switch (operator.kind) {
        case FIRST_VALUE:
        case LAST_VALUE:
        case NTILE:
          // These are handled with DruidSqlValidator and a rewrite rule.
          continue;
        default:
          assertFalse(
              operator + " allows framing; should be supported or rejected and then exclude from this check",
              operator.allowsFraming()
          );
      }
    }
  }
}
