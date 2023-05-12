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
import org.junit.Test;

public class ConfigsTest
{
  @Test
  public void testValueOrDefault()
  {
    Assert.assertEquals(10, Configs.valueOrDefault((Integer) 10, 11));
    Assert.assertEquals(11, Configs.valueOrDefault((Integer) null, 11));

    Assert.assertEquals(10, Configs.valueOrDefault((Long) 10L, 11L));
    Assert.assertEquals(11, Configs.valueOrDefault(null, 11L));

    Assert.assertFalse(Configs.valueOrDefault((Boolean) false, true));
    Assert.assertTrue(Configs.valueOrDefault(null, true));

    Assert.assertEquals("abc", Configs.valueOrDefault("abc", "def"));
    Assert.assertEquals("def", Configs.valueOrDefault(null, "def"));
  }

}
