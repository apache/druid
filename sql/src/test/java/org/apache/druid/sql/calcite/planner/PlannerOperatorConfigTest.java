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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class PlannerOperatorConfigTest
{
  @Test
  public void test_getDenyList_withNullList_returnsEmpty()
  {
    PlannerOperatorConfig config = PlannerOperatorConfig.newInstance(null);
    Assert.assertEquals(ImmutableList.of(), config.getDenyList());
  }

  @Test
  public void test_equals_withSameObject_returnsTrue()
  {
    PlannerOperatorConfig config = PlannerOperatorConfig.newInstance(ImmutableList.of("extern"));
    Assert.assertEquals("extern", config.getDenyList().get(0));
    Assert.assertEquals(config, config);
  }

  @Test
  public void test_equals_withConfigWithEqualDenyList_returnsTrue()
  {
    PlannerOperatorConfig config = PlannerOperatorConfig.newInstance(ImmutableList.of("extern"));
    PlannerOperatorConfig config2 = PlannerOperatorConfig.newInstance(ImmutableList.of("extern"));
    Assert.assertEquals(config, config2);
    Assert.assertEquals(config.hashCode(), config2.hashCode());
    Assert.assertEquals(config.toString(), config2.toString());
  }

  @Test
  public void test_equals_withDifferentObjectType_returnsFalse()
  {
    PlannerOperatorConfig config = PlannerOperatorConfig.newInstance(ImmutableList.of("extern"));
    Assert.assertNotEquals(config, 1L);
  }

  @Test
  public void test_equals_withNull_returnsFalse()
  {
    PlannerOperatorConfig config = PlannerOperatorConfig.newInstance(ImmutableList.of("extern"));
    Assert.assertNotEquals(config, null);
  }
}
