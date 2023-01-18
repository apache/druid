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

import org.apache.druid.catalog.model.MeasureTypes.MeasureType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class MeasureTypesTest
{
  @Test
  public void testInvalid()
  {
    try {
      MeasureTypes.parse("");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
    try {
      MeasureTypes.parse("FOO");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
    try {
      MeasureTypes.parse("FOO(");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
    try {
      MeasureTypes.parse("FOO)");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
    try {
      MeasureTypes.parse("MIN(,,)");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
    try {
      MeasureTypes.parse("MIN(VARCHAR");
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testCount()
  {
    MeasureType type = MeasureTypes.parse("count");
    assertSame(MeasureTypes.COUNT_TYPE, type);
    assertEquals(0, type.argTypes.size());

    type = MeasureTypes.parse("COUNT()");
    assertSame(MeasureTypes.COUNT_TYPE, type);
    assertEquals(0, type.argTypes.size());

    type = MeasureTypes.parse("  COUNT(  )  ");
    assertSame(MeasureTypes.COUNT_TYPE, type);
    assertEquals(0, type.argTypes.size());
  }

  @Test
  public void testSingleArg()
  {
    testOneArg("SUM");
    testOneArg("MIN");
    testOneArg("MAX");

    // Invalid

    assertThrows(IAE.class, () -> MeasureTypes.parse("SUM(VARCHAR)"));
    assertThrows(IAE.class, () -> MeasureTypes.parse("SUM()"));
    assertThrows(IAE.class, () -> MeasureTypes.parse("SUM(BIGINT, BIGINT)"));

    // Parsing variations
    MeasureType typeRef = MeasureTypes.parse("  Min  ( BiGiNt  )  ");
    assertSame(MeasureTypes.MIN_BIGINT_TYPE, typeRef);
  }

  private void testOneArg(String fn)
  {
    List<MeasureType> types = MeasureTypes.TYPES.get(fn);
    for (String name : Arrays.asList(fn, StringUtils.toLowerCase(fn))) {
      for (MeasureType measureType : types) {
        String argType = measureType.argTypes.get(0).name();
        for (String argName : Arrays.asList(argType, StringUtils.toLowerCase(argType))) {
          MeasureType typeRef = MeasureTypes.parse(StringUtils.format("%s(%s)", name, argName));
          assertSame(measureType, typeRef);
        }
      }
    }
  }
}
