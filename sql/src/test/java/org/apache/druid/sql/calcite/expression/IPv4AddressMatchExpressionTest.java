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
import org.apache.druid.sql.calcite.expression.builtin.IPv4AddressMatchOperatorConversion;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IPv4AddressMatchExpressionTest extends CalciteTestBase
{
  private static final String IPV4 = "192.168.0.1";
  private static final long IPV4_LONG = 3232235521L;
  private static final String IPV4_UINT = "3232235521";
  private static final String IPV4_NETWORK = "192.168.0.0";
  private static final String IPV4_BROADCAST = "192.168.255.255";
  private static final String IPV6_COMPATIBLE = "::192.168.0.1";
  private static final String IPV6_MAPPED = "::ffff:192.168.0.1";
  private static final String SUBNET_192_168 = "192.168.0.0/16";
  private static final String SUBNET_10 = "10.0.0.0/8";
  private static final Object IGNORE_EXPECTED_RESULT = null;
  private static final long MATCH = 1L;
  private static final long NO_MATCH = 0L;

  private static final String VAR = "s";
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder().add(VAR, ColumnType.STRING).build();
  private static final Map<String, Object> BINDINGS = ImmutableMap.of(VAR, "foo");

  private IPv4AddressMatchOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @Before
  public void setUp()
  {
    target = new IPv4AddressMatchOperatorConversion();
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
    Assert.assertEquals("Function[ipv4_match] requires 2 arguments", t.getMessage());
  }

  @Test
  public void testTooManyArgs()
  {
    String address = IPV4;
    String subnet = SUBNET_192_168;
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Arrays.asList(
                testHelper.makeLiteral(address),
                testHelper.makeLiteral(subnet),
                testHelper.makeLiteral(address)
            ),
            buildExpectedExpression(address, subnet, address),
            IGNORE_EXPECTED_RESULT
        )
    );
    Assert.assertEquals("Function[ipv4_match] requires 2 arguments", t.getMessage());
  }

  @Test
  public void testSubnetArgNotLiteral()
  {
    String address = IPV4;
    String variableName = VAR;
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Arrays.asList(
                testHelper.makeLiteral(address),
                testHelper.makeInputRef(variableName)
            ),
            buildExpectedExpression(address, testHelper.makeVariable(variableName)),
            IGNORE_EXPECTED_RESULT
        )
    );
    Assert.assertEquals("Function[ipv4_match] subnet argument must be a literal", t.getMessage());
  }

  @Test
  public void testSubnetArgInvalid()
  {
    String address = IPV4;
    String invalidSubnet = "192.168.0.1/invalid";
    Throwable t = Assert.assertThrows(
        ExpressionValidationException.class,
        () -> testExpression(
            Arrays.asList(
                testHelper.makeLiteral(address),
                testHelper.makeLiteral(invalidSubnet)
            ),
            buildExpectedExpression(address, invalidSubnet),
            IGNORE_EXPECTED_RESULT
        )
    );
    Assert.assertEquals("Function[ipv4_match] subnet arg has an invalid format: 192.168.0.1/invalid", t.getMessage());
  }

  @Test
  public void testNullArg()
  {
    String subnet = SUBNET_192_168;
    testExpression(
        Arrays.asList(
            testHelper.getConstantNull(),
            testHelper.makeLiteral(subnet)
        ),
        buildExpectedExpression(null, subnet),
        NO_MATCH
    );
  }

  @Test
  public void testInvalidArgType()
  {
    String variableNameWithInvalidType = VAR;
    String subnet = SUBNET_192_168;
    testExpression(
        Arrays.asList(
            testHelper.makeInputRef(variableNameWithInvalidType),
            testHelper.makeLiteral(subnet)
        ),
        buildExpectedExpression(testHelper.makeVariable(variableNameWithInvalidType), subnet),
        NO_MATCH
    );
  }

  @Test
  public void testMatchingStringArgIPv4()
  {
    testExpression(IPV4, SUBNET_192_168, MATCH);
  }

  @Test
  public void testNotMatchingStringArgIPv4()
  {
    testExpression(IPV4, SUBNET_10, NO_MATCH);
  }

  @Test
  public void testMatchingStringArgIPv6Mapped()
  {
    testExpression(IPV6_MAPPED, SUBNET_192_168, NO_MATCH);
  }

  @Test
  public void testNotMatchingStringArgIPv6Mapped()
  {
    testExpression(IPV6_MAPPED, SUBNET_10, NO_MATCH);
  }

  @Test
  public void testMatchingStringArgIPv6Compatible()
  {
    testExpression(IPV6_COMPATIBLE, SUBNET_192_168, NO_MATCH);
  }

  @Test
  public void testNotMatchingStringArgIPv6Compatible()
  {
    testExpression(IPV6_COMPATIBLE, SUBNET_10, NO_MATCH);
  }

  @Test
  public void testNotIpAddress()
  {
    testExpression("druid.apache.org", SUBNET_192_168, NO_MATCH);
  }

  @Test
  public void testMatchingLongArg()
  {
    testExpression(IPV4_LONG, SUBNET_192_168, MATCH);
  }

  @Test
  public void testNotMatchingLongArg()
  {
    testExpression(IPV4_LONG, SUBNET_10, NO_MATCH);
  }

  @Test
  public void testMatchingStringArgUnsignedInt()
  {
    testExpression(IPV4_UINT, SUBNET_192_168, NO_MATCH);
  }

  @Test
  public void testNotMatchingStringArgUnsignedInt()
  {
    testExpression(IPV4_UINT, SUBNET_10, NO_MATCH);
  }

  @Test
  public void testInclusive()
  {
    String subnet = SUBNET_192_168;
    testExpression(IPV4_NETWORK, subnet, MATCH);
    testExpression(IPV4, subnet, MATCH);
    testExpression(IPV4_BROADCAST, subnet, MATCH);
  }

  private void testExpression(String address, String subnet, long match)
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(address),
            testHelper.makeLiteral(subnet)
        ),
        buildExpectedExpression(address, subnet),
        match
    );
  }

  private void testExpression(long address, String subnet, long match)
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(address),
            testHelper.makeLiteral(subnet)
        ),
        buildExpectedExpression(address, subnet),
        match
    );
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
