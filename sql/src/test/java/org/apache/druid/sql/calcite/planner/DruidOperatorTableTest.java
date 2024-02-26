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
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DruidOperatorTableTest
{
  @Test
  void builtInOperatorTable()
  {
    DruidOperatorTable operatorTable = new DruidOperatorTable(ImmutableSet.of(), ImmutableSet.of());
    List<SqlOperator> operatorList = operatorTable.getOperatorList();
    assertNotNull(operatorList);
    assertTrue(operatorList.size() > 0, "Built-in operators should be loaded by default");
  }

  @Test
  void isFunctionSyntax()
  {
    assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION));
    assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION_STAR));
    assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.FUNCTION_ID));
    assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.SPECIAL));
    assertTrue(DruidOperatorTable.isFunctionSyntax(SqlSyntax.INTERNAL));

    assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.BINARY));
    assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.PREFIX));
    assertFalse(DruidOperatorTable.isFunctionSyntax(SqlSyntax.POSTFIX));
  }

  @Test
  void customOperatorTable()
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
    assertNotNull(operatorList);
    assertTrue(operatorList.size() > 2, "We should have at least two operators -- the ones we loaded above plus the built-in"
                      + " operators that gets loaded by default");

    assertTrue(operatorList.contains(operator1));
    assertTrue(operatorList.contains(operator2));

    assertTrue(DruidOperatorTable.isFunctionSyntax(operator1.getSyntax()));
    assertFalse(DruidOperatorTable.isFunctionSyntax(operator2.getSyntax()));
  }
}
