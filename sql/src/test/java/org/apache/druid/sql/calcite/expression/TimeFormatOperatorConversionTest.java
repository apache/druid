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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Tests for TIME_FORMAT
 */
public class TimeFormatOperatorConversionTest extends CalciteTestBase
{
  private static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("t", ColumnType.LONG)
      .build();
  private static final Map<String, Object> BINDINGS = ImmutableMap
      .<String, Object>builder()
      .put("t", DateTimes.of("2000-02-03T04:05:06").getMillis())
      .build();

  private TimeFormatOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @Before
  public void setUp()
  {
    target = new TimeFormatOperatorConversion();
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  public void testConversionToUTC()
  {
    testExpression(
        "2000-02-03 04:05:06",
        "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')",
        "yyyy-MM-dd HH:mm:ss",
        "UTC"
    );
  }

  @Test
  public void testConversionWithDefaultShouldUseUTC()
  {
    testExpression(
        "2000-02-03 04:05:06",
        "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','UTC')",
        "yyyy-MM-dd HH:mm:ss",
        null
    );
  }

  @Test
  public void testConversionToTimezone()
  {
    testExpression(
        "2000-02-02 20:05:06",
        "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')",
        "yyyy-MM-dd HH:mm:ss",
        "America/Los_Angeles"
    );
  }

  @Test(expected = IAE.class)
  public void testConversionToUnknownTimezoneShouldThrowException()
  {
    testExpression(
        "2000-02-02 20:05:06",
        "timestamp_format(\"t\",'yyyy-MM-dd HH:mm:ss','America/NO_TZ')",
        "yyyy-MM-dd HH:mm:ss",
        "America/NO_TZ"
    );
  }

  private void testExpression(
      String expectedResult,
      String expectedExpression,
      String format,
      @Nullable String timezone
  )
  {
    ImmutableList.Builder<RexNode> exprsBuilder = ImmutableList
        .<RexNode>builder()
        .add(testHelper.makeInputRef("t"))
        .add(testHelper.makeLiteral(format));
    if (timezone != null) {
      exprsBuilder.add(testHelper.makeLiteral(timezone));
    }

    testHelper.testExpressionString(
        target.calciteOperator(),
        exprsBuilder.build(),
        makeExpression(expectedExpression),
        expectedResult
    );
  }
}
