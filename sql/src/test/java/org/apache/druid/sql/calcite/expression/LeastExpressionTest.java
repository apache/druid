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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.builtin.LeastOperatorConversion;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LeastExpressionTest extends ExpressionTestBase
{
  private static final String DOUBLE_KEY = "d";
  private static final double DOUBLE_VALUE = 3.1;
  private static final String LONG_KEY = "l";
  private static final long LONG_VALUE = 2L;
  private static final String STRING_KEY = "s";
  private static final String STRING_VALUE = "foo";
  private static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add(DOUBLE_KEY, ValueType.DOUBLE)
      .add(LONG_KEY, ValueType.LONG)
      .add(STRING_KEY, ValueType.STRING)
      .build();
  private static final Map<String, Object> BINDINGS = ImmutableMap.of(
      DOUBLE_KEY, DOUBLE_VALUE,
      LONG_KEY, LONG_VALUE,
      STRING_KEY, STRING_VALUE
  );

  private LeastOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @Before
  public void setUp()
  {
    target = new LeastOperatorConversion();
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  public void testNoArgs()
  {
    testExpression(
        Collections.emptyList(),
        buildExpectedExpression(),
        null
    );
  }

  @Test
  public void testNull()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.getConstantNull(),
            testHelper.makeInputRef(STRING_KEY)
        ),
        buildExpectedExpression(
            testHelper.makeVariable(DOUBLE_KEY),
            null,
            testHelper.makeVariable(STRING_KEY)
        ),
        null
    );
  }

  @Test
  public void testAllDouble()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(34.1),
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.makeLiteral(5.2),
            testHelper.makeLiteral(767.3)
        ),
        buildExpectedExpression(
            34.1,
            testHelper.makeVariable(DOUBLE_KEY),
            5.2,
            767.3
        ),
        3.1
    );
  }

  @Test
  public void testAllLong()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeInputRef(LONG_KEY),
            testHelper.makeLiteral(0)
        ),
        buildExpectedExpression(
            testHelper.makeVariable(LONG_KEY),
            0
        ),
        0L
    );
  }

  @Test
  public void testAllString()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral("B"),
            testHelper.makeInputRef(STRING_KEY),
            testHelper.makeLiteral("A")
        ),
        buildExpectedExpression(
            "B",
            testHelper.makeVariable(STRING_KEY),
            "A"
        ),
        "A"
    );
  }

  @Test
  public void testCoerceString()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(-1),
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.makeLiteral("A")
        ),
        buildExpectedExpression(
            -1,
            testHelper.makeVariable(DOUBLE_KEY),
            "A"
        ),
        "-1"
    );
  }

  @Test
  public void testCoerceDouble()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(-1),
            testHelper.makeInputRef(DOUBLE_KEY)
        ),
        buildExpectedExpression(
            -1,
            testHelper.makeVariable(DOUBLE_KEY)
        ),
        -1.0
    );
  }

  private void testExpression(
      List<? extends RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testHelper.testExpression(target.calciteOperator(), exprs, expectedExpression, expectedResult);
  }

  private DruidExpression buildExpectedExpression(Object... args)
  {
    return testHelper.buildExpectedExpression(target.getDruidFunctionName(), args);
  }
}
