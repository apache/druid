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

package org.apache.druid.common.config;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.druid.common.config.NullHandling.replaceWithDefault;
import static org.junit.Assert.assertEquals;

public class NullHandlingTest
{
  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void test_defaultValueForClass_float()
  {
    assertEquals(
        replaceWithDefault() ? 0f : null,
        NullHandling.defaultValueForClass(Float.class)
    );
  }

  @Test
  public void test_defaultValueForClass_double()
  {
    assertEquals(
        replaceWithDefault() ? 0d : null,
        NullHandling.defaultValueForClass(Double.class)
    );
  }

  @Test
  public void test_defaultValueForClass_integer()
  {
    Assert.assertNull(NullHandling.defaultValueForClass(Integer.class));
  }

  @Test
  public void test_defaultValueForClass_long()
  {
    assertEquals(
        replaceWithDefault() ? 0L : null,
        NullHandling.defaultValueForClass(Long.class)
    );
  }

  @Test
  public void test_defaultValueForClass_number()
  {
    assertEquals(
        replaceWithDefault() ? 0d : null,
        NullHandling.defaultValueForClass(Number.class)
    );
  }

  @Test
  public void test_defaultValueForClass_string()
  {
    assertEquals(
        replaceWithDefault() ? "" : null,
        NullHandling.defaultValueForClass(String.class)
    );
  }

  @Test
  public void test_defaultValueForClass_object()
  {
    Assert.assertNull(NullHandling.defaultValueForClass(Object.class));
  }
}
