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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExprMacroTest
{
  private static final String IPV4_STRING = "192.168.0.1";
  private static final long IPV4_LONG = 3232235521L;
  private static final Expr.ObjectBinding BINDINGS = Parser.withMap(
      ImmutableMap.<String, Object>builder()
          .put("t", DateTimes.of("2000-02-03T04:05:06").getMillis())
          .put("t1", DateTimes.of("2000-02-03T00:00:00").getMillis())
          .put("tstr", "2000-02-03T04:05:06")
          .put("tstr_sql", "2000-02-03 04:05:06")
          .put("x", "foo")
          .put("y", 2)
          .put("z", 3.1)
          .put("CityOfAngels", "America/Los_Angeles")
          .put("spacey", "  hey there  ")
          .put("ipv4_string", IPV4_STRING)
          .put("ipv4_long", IPV4_LONG)
          .put("ipv4_network", "192.168.0.0")
          .put("ipv4_broadcast", "192.168.255.255")
          .build()
  );

  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLike()
  {
    assertExpr("like(x, 'f%')", 1L);
    assertExpr("like(x, 'f__')", 1L);
    assertExpr("like(x, '%o%')", 1L);
    assertExpr("like(x, 'b%')", 0L);
    assertExpr("like(x, 'b__')", 0L);
    assertExpr("like(x, '%x%')", 0L);
    assertExpr("like(x, '')", 0L);
  }

  @Test
  public void testLookup()
  {
    assertExpr("lookup(x, 'lookyloo')", "xfoo");
  }

  @Test
  public void testLookupNotFound()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Lookup [lookylook] not found");
    assertExpr("lookup(x, 'lookylook')", null);
  }

  @Test
  public void testRegexpExtract()
  {
    assertExpr("regexp_extract(x, 'f(.)')", "fo");
    assertExpr("regexp_extract(x, 'f(.)', 0)", "fo");
    assertExpr("regexp_extract(x, 'f(.)', 1)", "o");
  }

  @Test
  public void testTimestampCeil()
  {
    assertExpr("timestamp_ceil(t, 'P1M')", DateTimes.of("2000-03-01").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D',null,'America/Los_Angeles')", DateTimes.of("2000-02-03T08").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D',null,CityOfAngels)", DateTimes.of("2000-02-03T08").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D','1970-01-01T01','Etc/UTC')", DateTimes.of("2000-02-04T01").getMillis());
    assertExpr("timestamp_ceil(t1, 'P1D')", DateTimes.of("2000-02-03").getMillis());
  }

  @Test
  public void testTimestampFloor()
  {
    assertExpr("timestamp_floor(t, 'P1M')", DateTimes.of("2000-02-01").getMillis());
    assertExpr("timestamp_floor(t, 'P1D',null,'America/Los_Angeles')", DateTimes.of("2000-02-02T08").getMillis());
    assertExpr("timestamp_floor(t, 'P1D',null,CityOfAngels)", DateTimes.of("2000-02-02T08").getMillis());
    assertExpr("timestamp_floor(t, 'P1D','1970-01-01T01','Etc/UTC')", DateTimes.of("2000-02-03T01").getMillis());
  }

  @Test
  public void testTimestampShift()
  {
    assertExpr("timestamp_shift(t, 'P1D', 2)", DateTimes.of("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, 'America/Los_Angeles')", DateTimes.of("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, CityOfAngels)", DateTimes.of("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, '-08:00')", DateTimes.of("2000-02-05T04:05:06").getMillis());
  }

  @Test
  public void testTimestampExtract()
  {
    assertExpr("timestamp_extract(t, 'DAY')", 3L);
    assertExpr("timestamp_extract(t, 'HOUR')", 4L);
    assertExpr("timestamp_extract(t, 'DAY', 'America/Los_Angeles')", 2L);
    assertExpr("timestamp_extract(t, 'HOUR', 'America/Los_Angeles')", 20L);
  }

  @Test
  public void testTimestampParse()
  {
    assertExpr("timestamp_parse(tstr)", DateTimes.of("2000-02-03T04:05:06").getMillis());
    assertExpr("timestamp_parse(tstr_sql)", DateTimes.of("2000-02-03T04:05:06").getMillis());
    assertExpr(
        "timestamp_parse(tstr_sql,null,'America/Los_Angeles')",
        DateTimes.of("2000-02-03T04:05:06-08:00").getMillis()
    );
    assertExpr("timestamp_parse('2000-02-03')", DateTimes.of("2000-02-03").getMillis());
    assertExpr("timestamp_parse('2000-02')", DateTimes.of("2000-02-01").getMillis());
    assertExpr("timestamp_parse(null)", null);
    assertExpr("timestamp_parse('z2000')", null);
    assertExpr("timestamp_parse(tstr_sql,'yyyy-MM-dd HH:mm:ss')", DateTimes.of("2000-02-03T04:05:06").getMillis());
    assertExpr("timestamp_parse('02/03/2000','MM/dd/yyyy')", DateTimes.of("2000-02-03").getMillis());
    assertExpr(
        "timestamp_parse(tstr_sql,'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')",
        DateTimes.of("2000-02-03T04:05:06-08:00").getMillis()
    );
  }

  @Test
  public void testTimestampFormat()
  {
    assertExpr("timestamp_format(t)", "2000-02-03T04:05:06.000Z");
    assertExpr("timestamp_format(t,'yyyy-MM-dd HH:mm:ss')", "2000-02-03 04:05:06");
    assertExpr("timestamp_format(t,'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')", "2000-02-02 20:05:06");
  }

  @Test
  public void testTrim()
  {
    String emptyString = NullHandling.replaceWithDefault() ? null : "";
    assertExpr("trim('')", emptyString);
    assertExpr("trim(concat(' ',x,' '))", "foo");
    assertExpr("trim(spacey)", "hey there");
    assertExpr("trim(spacey, '')", "  hey there  ");
    assertExpr("trim(spacey, 'he ')", "y ther");
    assertExpr("trim(spacey, spacey)", emptyString);
    assertExpr("trim(spacey, substring(spacey, 0, 4))", "y ther");
  }

  @Test
  public void testLTrim()
  {
    String emptyString = NullHandling.replaceWithDefault() ? null : "";
    assertExpr("ltrim('')", emptyString);
    assertExpr("ltrim(concat(' ',x,' '))", "foo ");
    assertExpr("ltrim(spacey)", "hey there  ");
    assertExpr("ltrim(spacey, '')", "  hey there  ");
    assertExpr("ltrim(spacey, 'he ')", "y there  ");
    assertExpr("ltrim(spacey, spacey)", emptyString);
    assertExpr("ltrim(spacey, substring(spacey, 0, 4))", "y there  ");
  }

  @Test
  public void testRTrim()
  {
    String emptyString = NullHandling.replaceWithDefault() ? null : "";
    assertExpr("rtrim('')", emptyString);
    assertExpr("rtrim(concat(' ',x,' '))", " foo");
    assertExpr("rtrim(spacey)", "  hey there");
    assertExpr("rtrim(spacey, '')", "  hey there  ");
    assertExpr("rtrim(spacey, 'he ')", "  hey ther");
    assertExpr("rtrim(spacey, spacey)", emptyString);
    assertExpr("rtrim(spacey, substring(spacey, 0, 4))", "  hey ther");
  }

  @Test
  public void testIPv4AddressParse()
  {
    Long nullLong = NullHandling.replaceWithDefault() ? NullHandling.ZERO_LONG : null;
    assertExpr("ipv4_parse(x)", nullLong);
    assertExpr("ipv4_parse(ipv4_string)", IPV4_LONG);
    assertExpr("ipv4_parse(ipv4_long)", IPV4_LONG);
    assertExpr("ipv4_parse(ipv4_stringify(ipv4_long))", IPV4_LONG);
  }

  @Test
  public void testIPv4AddressStringify()
  {
    assertExpr("ipv4_stringify(x)", null);
    assertExpr("ipv4_stringify(ipv4_long)", IPV4_STRING);
    assertExpr("ipv4_stringify(ipv4_string)", IPV4_STRING);
    assertExpr("ipv4_stringify(ipv4_parse(ipv4_string))", IPV4_STRING);
  }

  @Test
  public void testIPv4AddressMatch()
  {
    assertExpr("ipv4_match(ipv4_string,    '10.0.0.0/8')", 0L);
    assertExpr("ipv4_match(ipv4_string,    '192.168.0.0/16')", 1L);
    assertExpr("ipv4_match(ipv4_network,   '192.168.0.0/16')", 1L);
    assertExpr("ipv4_match(ipv4_broadcast, '192.168.0.0/16')", 1L);
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(expression, expectedResult, expr.eval(BINDINGS).value());

    final Expr exprNotFlattened = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE, false);
    final Expr roundTripNotFlattened =
        Parser.parse(exprNotFlattened.stringify(), LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTripNotFlattened.eval(BINDINGS).value());

    final Expr roundTrip = Parser.parse(expr.stringify(), LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTrip.eval(BINDINGS).value());
  }
}
