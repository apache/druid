/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.expression;

import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExprMacroTest
{
  private static final Expr.ObjectBinding BINDINGS = Parser.withMap(
      ImmutableMap.<String, Object>builder()
          .put("t", new DateTime("2000-02-03T04:05:06").getMillis())
          .put("tstr", "2000-02-03T04:05:06")
          .put("tstr_sql", "2000-02-03 04:05:06")
          .put("x", "foo")
          .put("y", 2)
          .put("z", 3.1)
          .put("CityOfAngels", "America/Los_Angeles")
          .build()
  );

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
    expectedException.expect(NullPointerException.class);
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
    assertExpr("timestamp_ceil(t, 'P1M')", new DateTime("2000-03-01").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D','','America/Los_Angeles')", new DateTime("2000-02-03T08").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D','',CityOfAngels)", new DateTime("2000-02-03T08").getMillis());
    assertExpr("timestamp_ceil(t, 'P1D','1970-01-01T01','Etc/UTC')", new DateTime("2000-02-04T01").getMillis());
  }

  @Test
  public void testTimestampFloor()
  {
    assertExpr("timestamp_floor(t, 'P1M')", new DateTime("2000-02-01").getMillis());
    assertExpr("timestamp_floor(t, 'P1D','','America/Los_Angeles')", new DateTime("2000-02-02T08").getMillis());
    assertExpr("timestamp_floor(t, 'P1D','',CityOfAngels)", new DateTime("2000-02-02T08").getMillis());
    assertExpr("timestamp_floor(t, 'P1D','1970-01-01T01','Etc/UTC')", new DateTime("2000-02-03T01").getMillis());
  }

  @Test
  public void testTimestampShift()
  {
    assertExpr("timestamp_shift(t, 'P1D', 2)", new DateTime("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, 'America/Los_Angeles')", new DateTime("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, CityOfAngels)", new DateTime("2000-02-05T04:05:06").getMillis());
    assertExpr("timestamp_shift(t, 'P1D', 2, '-08:00')", new DateTime("2000-02-05T04:05:06").getMillis());
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
    assertExpr("timestamp_parse(tstr)", new DateTime("2000-02-03T04:05:06").getMillis());
    assertExpr("timestamp_parse(tstr_sql)", null);
    assertExpr("timestamp_parse(tstr_sql,'yyyy-MM-dd HH:mm:ss')", new DateTime("2000-02-03T04:05:06").getMillis());
    assertExpr(
        "timestamp_parse(tstr_sql,'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')",
        new DateTime("2000-02-03T04:05:06-08:00").getMillis()
    );
  }

  @Test
  public void testTimestampFormat()
  {
    assertExpr("timestamp_format(t)", "2000-02-03T04:05:06.000Z");
    assertExpr("timestamp_format(t,'yyyy-MM-dd HH:mm:ss')", "2000-02-03 04:05:06");
    assertExpr("timestamp_format(t,'yyyy-MM-dd HH:mm:ss','America/Los_Angeles')", "2000-02-02 20:05:06");
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, TestExprMacroTable.INSTANCE);
    Assert.assertEquals(expression, expectedResult, expr.eval(BINDINGS).value());
  }
}
