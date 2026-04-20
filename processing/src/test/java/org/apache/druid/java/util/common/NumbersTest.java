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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NumbersTest
{
  @Test
  public void testParseLong()
  {
    final String strVal = "100";
    Assertions.assertEquals(100L, Numbers.parseLong(strVal));

    final Long longVal = 100L;
    Assertions.assertEquals(100L, Numbers.parseLong(longVal));

    final Double doubleVal = 100.;
    Assertions.assertEquals(100L, Numbers.parseLong(doubleVal));
  }

  @Test
  public void testParseLongWithNull()
  {
    NullPointerException e = Assertions.assertThrows(NullPointerException.class, () -> Numbers.parseLong(null));
    Assertions.assertTrue(e.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseLongWithUnparseableString()
  {
    Assertions.assertThrows(NumberFormatException.class, () -> Numbers.parseLong("unparseable"));
  }

  @Test
  public void testParseLongWithUnparseableObject()
  {
    ISE e = Assertions.assertThrows(ISE.class, () -> Numbers.parseLong(new Object()));
    Assertions.assertTrue(e.getMessage().startsWith("Unknown type"));
  }

  @Test
  public void testParseInt()
  {
    final String strVal = "100";
    Assertions.assertEquals(100, Numbers.parseInt(strVal));

    final Integer longVal = 100;
    Assertions.assertEquals(100, Numbers.parseInt(longVal));

    final Float floatVal = 100.F;
    Assertions.assertEquals(100, Numbers.parseInt(floatVal));
  }

  @Test
  public void testParseIntWithNull()
  {
    NullPointerException e = Assertions.assertThrows(NullPointerException.class, () -> Numbers.parseInt(null));
    Assertions.assertTrue(e.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseIntWithUnparseableString()
  {
    Assertions.assertThrows(NumberFormatException.class, () -> Numbers.parseInt("unparseable"));
  }

  @Test
  public void testParseIntWithUnparseableObject()
  {
    ISE e = Assertions.assertThrows(ISE.class, () -> Numbers.parseInt(new Object()));
    Assertions.assertTrue(e.getMessage().startsWith("Unknown type"));
  }

  @Test
  public void testParseBoolean()
  {
    final String strVal = "false";
    Assertions.assertFalse(Numbers.parseBoolean(strVal));

    final Boolean booleanVal = Boolean.FALSE;
    Assertions.assertFalse(Numbers.parseBoolean(booleanVal));
  }

  @Test
  public void testParseBooleanWithNull()
  {
    NullPointerException e = Assertions.assertThrows(NullPointerException.class, () -> Numbers.parseBoolean(null));
    Assertions.assertTrue(e.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseBooleanWithUnparseableObject()
  {
    ISE e = Assertions.assertThrows(ISE.class, () -> Numbers.parseBoolean(new Object()));
    Assertions.assertTrue(e.getMessage().startsWith("Unknown type"));
  }

  @Test
  public void testParseLongObject()
  {
    Assertions.assertNull(Numbers.parseLongObject(null));
    Assertions.assertEquals((Long) 1L, Numbers.parseLongObject("1"));
    Assertions.assertEquals((Long) 32L, Numbers.parseLongObject("32.1243"));
  }

  @Test
  public void testParseLongObjectUnparseable()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Numbers.parseLongObject("'1'")
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot parse string to long"));
  }

  @Test
  public void testParseDoubleObject()
  {
    Assertions.assertNull(Numbers.parseLongObject(null));
    Assertions.assertEquals((Double) 1.0, Numbers.parseDoubleObject("1"));
    Assertions.assertEquals((Double) 32.1243, Numbers.parseDoubleObject("32.1243"));
  }

  @Test
  public void testParseDoubleObjectUnparseable()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Numbers.parseDoubleObject("'1.1'")
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot parse string to double"));
  }
}
