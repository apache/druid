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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IPv4AddressParseExpressionTest extends CalciteTestBase
{
  private static final String VALID = "192.168.0.1";
  private static final long EXPECTED = 3232235521L;
  private static final Object IGNORE_EXPECTED_RESULT = null;

  private static final String VAR = "f";
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder().add(VAR, ColumnType.FLOAT).build();
  private static final Map<String, Object> BINDINGS = ImmutableMap.of(VAR, 3.14);

  private IPv4AddressParseOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @Before
  public void setUp()
  {
    target = new IPv4AddressParseOperatorConversion();
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  public void testTooFewArgs()
  {
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Collections.emptyList(),
            buildExpectedExpression(),
            IGNORE_EXPECTED_RESULT
        )
    );
    Assert.assertEquals("Function[ipv4_parse] requires 1 argument", t.getMessage());
  }

  @Test
  public void testTooManyArgs()
  {
    Throwable t = Assert.assertThrows(
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
    Assert.assertEquals("Function[ipv4_parse] requires 1 argument", t.getMessage());
  }

  @Test
  public void testNullArg()
  {
    testExpression(
        testHelper.getConstantNull(),
        buildExpectedExpression((String) null),
        null
    );
  }

  @Test
  public void testInvalidArgType()
  {
    String variableNameWithInvalidType = VAR;
    testExpression(
        testHelper.makeInputRef(variableNameWithInvalidType),
        buildExpectedExpression(testHelper.makeVariable(variableNameWithInvalidType)),
        null
    );
  }

  @Test
  public void testInvalidStringArgNotIPAddress()
  {
    String notIpAddress = "druid.apache.org";
    testExpression(
        testHelper.makeLiteral(notIpAddress),
        buildExpectedExpression(notIpAddress),
        null
    );
  }

  @Test
  public void testInvalidStringArgIPv6Compatible()
  {
    String ipv6Compatible = "::192.168.0.1";
    testExpression(
        testHelper.makeLiteral(ipv6Compatible),
        buildExpectedExpression(ipv6Compatible),
        null
    );
  }

  @Test
  public void testValidStringArgIPv6Mapped()
  {
    String ipv6Mapped = "::ffff:192.168.0.1";
    testExpression(
        testHelper.makeLiteral(ipv6Mapped),
        buildExpectedExpression(ipv6Mapped),
        null
    );
  }

  @Test
  public void testValidStringArgIPv4()
  {
    testExpression(
        testHelper.makeLiteral(VALID),
        buildExpectedExpression(VALID),
        EXPECTED
    );
  }

  @Test
  public void testValidStringArgUnsignedInt()
  {
    String unsignedInt = "3232235521";
    testExpression(
        testHelper.makeLiteral(unsignedInt),
        buildExpectedExpression(unsignedInt),
        null
    );
  }

  @Test
  public void testInvalidIntegerArgTooLow()
  {
    long tooLow = -1L;
    testExpression(
        testHelper.makeLiteral(tooLow),
        buildExpectedExpression(tooLow),
        null
    );
  }

  @Test
  public void testValidIntegerArgLowest()
  {
    long lowest = 0L;
    testExpression(
        testHelper.makeLiteral(lowest),
        buildExpectedExpression(lowest),
        lowest
    );
  }

  @Test
  public void testValidIntegerArg()
  {
    testExpression(
        testHelper.makeLiteral(EXPECTED),
        buildExpectedExpression(EXPECTED),
        EXPECTED
    );
  }

  @Test
  public void testValidIntegerArgHighest()
  {
    long highest = 0xff_ff_ff_ffL;
    testExpression(
        testHelper.makeLiteral(highest),
        buildExpectedExpression(highest),
        highest
    );
  }

  @Test
  public void testInvalidIntegerArgTooHigh()
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
