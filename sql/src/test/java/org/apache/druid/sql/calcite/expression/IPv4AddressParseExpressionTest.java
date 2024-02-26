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

package org.apache.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.builtin.IPv4AddressParseOperatorConversion;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IPv4AddressParseExpressionTest extends CalciteTestBase
{
  private static final String VALID = "192.168.0.1";
  private static final long EXPECTED = 3232235521L;
  private static final Object IGNORE_EXPECTED_RESULT = null;

  private static final String VAR = "f";
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder().add(VAR, ColumnType.FLOAT).build();
  private static final Map<String, Object> BINDINGS = ImmutableMap.of(VAR, 3.14);

  private IPv4AddressParseOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @BeforeEach
  void setUp()
  {
    target = new IPv4AddressParseOperatorConversion();
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  void tooFewArgs()
  {
    Throwable t = assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Collections.emptyList(),
            buildExpectedExpression(),
            IGNORE_EXPECTED_RESULT
        )
    );
    assertEquals("Function[ipv4_parse] requires 1 argument", t.getMessage());
  }

  @Test
  void tooManyArgs()
  {
    Throwable t = assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Arrays.asList(
                testHelper.getConstantNull(),
                testHelper.getConstantNull()
            ),
            buildExpectedExpression(null, null),
            IGNORE_EXPECTED_RESULT
        )
    );
    assertEquals("Function[ipv4_parse] requires 1 argument", t.getMessage());
  }

  @Test
  void nullArg()
  {
    testExpression(
        testHelper.getConstantNull(),
        buildExpectedExpression((String) null),
        null
    );
  }

  @Test
  void invalidArgType()
  {
    String variableNameWithInvalidType = VAR;
    testExpression(
        testHelper.makeInputRef(variableNameWithInvalidType),
        buildExpectedExpression(testHelper.makeVariable(variableNameWithInvalidType)),
        null
    );
  }

  @Test
  void invalidStringArgNotIPAddress()
  {
    String notIpAddress = "druid.apache.org";
    testExpression(
        testHelper.makeLiteral(notIpAddress),
        buildExpectedExpression(notIpAddress),
        null
    );
  }

  @Test
  void invalidStringArgIPv6Compatible()
  {
    String ipv6Compatible = "::192.168.0.1";
    testExpression(
        testHelper.makeLiteral(ipv6Compatible),
        buildExpectedExpression(ipv6Compatible),
        null
    );
  }

  @Test
  void validStringArgIPv6Mapped()
  {
    String ipv6Mapped = "::ffff:192.168.0.1";
    testExpression(
        testHelper.makeLiteral(ipv6Mapped),
        buildExpectedExpression(ipv6Mapped),
        null
    );
  }

  @Test
  void validStringArgIPv4()
  {
    testExpression(
        testHelper.makeLiteral(VALID),
        buildExpectedExpression(VALID),
        EXPECTED
    );
  }

  @Test
  void validStringArgUnsignedInt()
  {
    String unsignedInt = "3232235521";
    testExpression(
        testHelper.makeLiteral(unsignedInt),
        buildExpectedExpression(unsignedInt),
        null
    );
  }

  @Test
  void invalidIntegerArgTooLow()
  {
    long tooLow = -1L;
    testExpression(
        testHelper.makeLiteral(tooLow),
        buildExpectedExpression(tooLow),
        null
    );
  }

  @Test
  void validIntegerArgLowest()
  {
    long lowest = 0L;
    testExpression(
        testHelper.makeLiteral(lowest),
        buildExpectedExpression(lowest),
        lowest
    );
  }

  @Test
  void validIntegerArg()
  {
    testExpression(
        testHelper.makeLiteral(EXPECTED),
        buildExpectedExpression(EXPECTED),
        EXPECTED
    );
  }

  @Test
  void validIntegerArgHighest()
  {
    long highest = 0xff_ff_ff_ffL;
    testExpression(
        testHelper.makeLiteral(highest),
        buildExpectedExpression(highest),
        highest
    );
  }

  @Test
  void invalidIntegerArgTooHigh()
  {
    long tooHigh = 0x1_00_00_00_00L;
    testExpression(
        testHelper.makeLiteral(tooHigh),
        buildExpectedExpression(tooHigh),
        null
    );
  }

  private void testExpression(
      RexNode expr,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testExpression(Collections.singletonList(expr), expectedExpression, expectedResult);
  }

  private void testExpression(
      List<? extends RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testHelper.testExpressionString(target.calciteOperator(), exprs, expectedExpression, expectedResult);
  }

  private DruidExpression buildExpectedExpression(Object... args)
  {
    return testHelper.buildExpectedExpression(target.getDruidFunctionName(), args);
  }
}
