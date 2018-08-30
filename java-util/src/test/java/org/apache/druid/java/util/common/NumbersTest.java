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

package org.apache.druid.java.util.common;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NumbersTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseLong()
  {
    final String strVal = "100";
    Assert.assertEquals(100L, Numbers.parseLong(strVal));

    final Long longVal = 100L;
    Assert.assertEquals(100L, Numbers.parseLong(longVal));

    final Double doubleVal = 100.;
    Assert.assertEquals(100L, Numbers.parseLong(doubleVal));
  }

  @Test
  public void testParseLongWithNull()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Input is null");
    Numbers.parseLong(null);
  }

  @Test
  public void testParseLongWithUnparseableString()
  {
    expectedException.expect(NumberFormatException.class);
    Numbers.parseLong("unparseable");
  }

  @Test
  public void testParseLongWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseLong(new Object());
  }

  @Test
  public void testParseInt()
  {
    final String strVal = "100";
    Assert.assertEquals(100, Numbers.parseInt(strVal));

    final Integer longVal = 100;
    Assert.assertEquals(100, Numbers.parseInt(longVal));

    final Float floatVal = 100.F;
    Assert.assertEquals(100, Numbers.parseInt(floatVal));
  }

  @Test
  public void testParseIntWithNull()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Input is null");
    Numbers.parseInt(null);
  }

  @Test
  public void testParseIntWithUnparseableString()
  {
    expectedException.expect(NumberFormatException.class);
    Numbers.parseInt("unparseable");
  }

  @Test
  public void testParseIntWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseInt(new Object());
  }

  @Test
  public void testParseBoolean()
  {
    final String strVal = "false";
    Assert.assertEquals(false, Numbers.parseBoolean(strVal));

    final Boolean booleanVal = Boolean.FALSE;
    Assert.assertEquals(false, Numbers.parseBoolean(booleanVal));
  }

  @Test
  public void testParseBooleanWithNull()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("Input is null");
    Numbers.parseBoolean(null);
  }

  @Test
  public void testParseBooleanWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseBoolean(new Object());
  }
}
