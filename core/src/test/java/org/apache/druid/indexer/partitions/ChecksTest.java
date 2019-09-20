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

package org.apache.druid.indexer.partitions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class ChecksTest
{
  private static final String NAME1 = "name1";
  private static final Integer VALUE1 = 1;
  private static final String NAME2 = "name2";
  private static final Integer VALUE2 = 2;
  private static final Integer NULL = null;
  private static final Integer HISTORICAL_NULL = -1;

  @Parameterized.Parameter
  public Integer nullValue;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Parameterized.Parameters(name = "{index}: nullValue={0}")
  public static Iterable<? extends Object> nullValues()
  {
    return Arrays.asList(NULL, HISTORICAL_NULL);
  }

  @Test
  public void checkAtMostOneNotNullFirstNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, nullValue, NAME2, VALUE2);
    Assert.assertEquals(NAME2, result.getName());
    Assert.assertEquals(VALUE2, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullSecondNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, VALUE1, NAME2, nullValue);
    Assert.assertEquals(NAME1, result.getName());
    Assert.assertEquals(VALUE1, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullBothNull()
  {
    Property<Integer> result = Checks.checkAtMostOneNotNull(NAME1, nullValue, NAME2, nullValue);
    Assert.assertEquals(NAME1, result.getName());
    Assert.assertEquals(nullValue, result.getValue());
  }

  @Test
  public void checkAtMostOneNotNullNeitherNull()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("At most one of " + NAME1 + " or " + NAME2 + " must be present");

    //noinspection ConstantConditions (expected to fail)
    Checks.checkAtMostOneNotNull(NAME1, VALUE1, NAME2, VALUE2);
  }
}
