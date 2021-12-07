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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

@RunWith(Enclosed.class)
public class DruidLogicalValuesRuleTest
{
  private static final PlannerContext DEFAULT_CONTEXT = Mockito.mock(PlannerContext.class);

  @RunWith(Parameterized.class)
  public static class GetValueFromLiteralSimpleTypesTest
  {
    @Parameters(name = "{1}, {2}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{"test", SqlTypeName.CHAR, String.class},
          new Object[]{"test", SqlTypeName.VARCHAR, String.class},
          new Object[]{0.1, SqlTypeName.DOUBLE, Double.class},
          new Object[]{0.1, SqlTypeName.REAL, Double.class},
          new Object[]{0.1, SqlTypeName.DECIMAL, Double.class},
          new Object[]{1L, SqlTypeName.TINYINT, Long.class},
          new Object[]{1L, SqlTypeName.SMALLINT, Long.class},
          new Object[]{1L, SqlTypeName.INTEGER, Long.class},
          new Object[]{1L, SqlTypeName.BIGINT, Long.class}
      );
    }

    private final Object val;
    private final SqlTypeName sqlTypeName;
    private final Class<?> javaType;

    public GetValueFromLiteralSimpleTypesTest(Object val, SqlTypeName sqlTypeName, Class<?> javaType)
    {
      this.val = val;
      this.sqlTypeName = sqlTypeName;
      this.javaType = javaType;
    }

    @Test
    public void testGetValueFromLiteral()
    {
      final RexLiteral literal = makeLiteral(val, sqlTypeName, javaType);
      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assert.assertSame(javaType, fromLiteral.getClass());
      Assert.assertEquals(val, fromLiteral);
      Mockito.verify(literal, Mockito.times(1)).getType();
      Mockito.verify(literal, Mockito.times(1)).getValueAs(ArgumentMatchers.any());
    }

    private static RexLiteral makeLiteral(Object val, SqlTypeName typeName, Class<?> javaType)
    {
      RelDataType dataType = Mockito.mock(RelDataType.class);
      Mockito.when(dataType.getSqlTypeName()).thenReturn(typeName);
      RexLiteral literal = Mockito.mock(RexLiteral.class);
      Mockito.when(literal.getType()).thenReturn(dataType);
      Mockito.when(literal.getValueAs(ArgumentMatchers.any())).thenReturn(javaType.cast(val));
      return literal;
    }
  }

  public static class GetValueFromLiteralOtherTypesTest
  {
    private static final PlannerContext DEFAULT_CONTEXT = Mockito.mock(PlannerContext.class);
    private static final DateTimeZone TIME_ZONE = DateTimes.inferTzFromString("Asia/Seoul");
    private static final RexBuilder REX_BUILDER = new RexBuilder(new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setup()
    {
      Mockito.when(DEFAULT_CONTEXT.getTimeZone()).thenReturn(TIME_ZONE);
    }

    @Test
    public void testGetValueFromTrueLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeLiteral(true);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assert.assertSame(Long.class, fromLiteral.getClass());
      Assert.assertEquals(1L, fromLiteral);
    }

    @Test
    public void testGetValueFromFalseLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeLiteral(false);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assert.assertSame(Long.class, fromLiteral.getClass());
      Assert.assertEquals(0L, fromLiteral);
    }

    @Test
    public void testGetValueFromTimestampLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimestampLiteral(new TimestampString("2021-04-01 16:54:31"), 0);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assert.assertSame(Long.class, fromLiteral.getClass());
      Assert.assertEquals(new DateTime("2021-04-01T16:54:31", TIME_ZONE).getMillis(), fromLiteral);
    }

    @Test
    public void testGetValueFromDateLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeDateLiteral(new DateString("2021-04-01"));

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assert.assertSame(Long.class, fromLiteral.getClass());
      Assert.assertEquals(new DateTime("2021-04-01", TIME_ZONE).getMillis(), fromLiteral);
    }

    @Test
    public void testGetValueFromTimestampWithLocalTimeZoneLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimestampWithLocalTimeZoneLiteral(
          new TimestampString("2021-04-01 16:54:31"),
          0
      );
      expectedException.expect(UnsupportedSQLQueryException.class);
      expectedException.expectMessage("TIMESTAMP_WITH_LOCAL_TIME_ZONE type is not supported");
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }

    @Test
    public void testGetValueFromTimeLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimeLiteral(new TimeString("16:54:31"), 0);
      expectedException.expect(UnsupportedSQLQueryException.class);
      expectedException.expectMessage("TIME type is not supported");
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }

    @Test
    public void testGetValueFromTimeWithLocalTimeZoneLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimeWithLocalTimeZoneLiteral(new TimeString("16:54:31"), 0);
      expectedException.expect(UnsupportedSQLQueryException.class);
      expectedException.expectMessage("TIME_WITH_LOCAL_TIME_ZONE type is not supported");
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }
  }
}
