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

package org.apache.druid.catalog.model;

import org.apache.druid.catalog.model.TypeParser.ParsedType;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class TypeParserTest
{
  @Test
  public void testScalars()
  {
    ParsedType type = TypeParser.parse(null);
    assertEquals(ParsedType.Kind.ANY, type.kind());
    assertNull(type.type());

    type = TypeParser.parse("");
    assertEquals(ParsedType.Kind.ANY, type.kind());
    assertNull(type.type());

    type = TypeParser.parse("   ");
    assertEquals(ParsedType.Kind.ANY, type.kind());
    assertNull(type.type());

    type = TypeParser.parse(Columns.VARCHAR);
    assertEquals(ParsedType.Kind.DIMENSION, type.kind());
    assertEquals(Columns.VARCHAR, type.type());

    type = TypeParser.parse("  VARCHAR  ");
    assertEquals(ParsedType.Kind.DIMENSION, type.kind());
    assertEquals(Columns.VARCHAR, type.type());

    type = TypeParser.parse("varchar");
    assertEquals(ParsedType.Kind.DIMENSION, type.kind());
    assertEquals(Columns.VARCHAR, type.type());

    type = TypeParser.parse("vArChAr");
    assertEquals(ParsedType.Kind.DIMENSION, type.kind());
    assertEquals(Columns.VARCHAR, type.type());

    assertThrows(IAE.class, () -> TypeParser.parse("foo"));
  }

  @Test
  public void testTime()
  {
    ParsedType type = TypeParser.parse(Columns.TIMESTAMP);
    assertEquals(ParsedType.Kind.TIME, type.kind());
    assertNull(type.timeGrain());

    type = TypeParser.parse("timestamp()");
    assertEquals(ParsedType.Kind.TIME, type.kind());
    assertNull(type.timeGrain());

    type = TypeParser.parse("timestamp('PT5M')");
    assertEquals(ParsedType.Kind.TIME, type.kind());
    assertEquals("PT5M", type.timeGrain());

    assertThrows(IAE.class, () -> TypeParser.parse("timestamp("));
    assertThrows(IAE.class, () -> TypeParser.parse("timestamp('PT5M'"));
    assertThrows(IAE.class, () -> TypeParser.parse("timestamp(BIGINT)"));
    assertThrows(IAE.class, () -> TypeParser.parse("timestamp('bogus')"));
    assertThrows(IAE.class, () -> TypeParser.parse("timestamp('PT5M)"));
  }

  @Test
  public void testCount()
  {
    ParsedType type = TypeParser.parse("COUNT");
    assertEquals(ParsedType.Kind.MEASURE, type.kind());
    assertSame(MeasureTypes.COUNT_TYPE, type.measure());

    type = TypeParser.parse("COUNT()");
    assertEquals(ParsedType.Kind.MEASURE, type.kind());
    assertSame(MeasureTypes.COUNT_TYPE, type.measure());

    type = TypeParser.parse(" COUNT ( ) ");
    assertEquals(ParsedType.Kind.MEASURE, type.kind());
    assertSame(MeasureTypes.COUNT_TYPE, type.measure());

    assertThrows(IAE.class, () -> TypeParser.parse("count("));
    assertThrows(IAE.class, () -> TypeParser.parse("count(BIGINT)"));
  }

  @Test
  public void testSum()
  {
    ParsedType type = TypeParser.parse("sum(bigint)");
    assertEquals(ParsedType.Kind.MEASURE, type.kind());
    assertSame(MeasureTypes.SUM_BIGINT_TYPE, type.measure());

    type = TypeParser.parse(" SUM ( BIGINT ) ");
    assertEquals(ParsedType.Kind.MEASURE, type.kind());
    assertSame(MeasureTypes.SUM_BIGINT_TYPE, type.measure());

    assertThrows(IAE.class, () -> TypeParser.parse("sum("));
    assertThrows(IAE.class, () -> TypeParser.parse("sum(BIGINT,)"));
    assertThrows(IAE.class, () -> TypeParser.parse("sum(BIGINT,BIGINT)"));
  }
}
