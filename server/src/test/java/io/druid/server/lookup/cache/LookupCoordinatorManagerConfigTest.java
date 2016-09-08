/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.lookup.cache;

import io.druid.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class LookupCoordinatorManagerConfigTest
{
  @Test
  public void testConfigsTakeOverrides()
  {
    final Duration funnyDuration = Duration.standardDays(100);
    final LookupCoordinatorManagerConfig config = new LookupCoordinatorManagerConfig();
    config.setDeleteAllTimeout(funnyDuration);
    config.setHostDeleteTimeout(funnyDuration);
    config.setHostUpdateTimeout(funnyDuration);
    config.setUpdateAllTimeout(funnyDuration);
    config.setPeriod(funnyDuration.getMillis());
    config.setThreadPoolSize(1200);

    Assert.assertEquals(funnyDuration, config.getDeleteAllTimeout());
    Assert.assertEquals(funnyDuration, config.getHostDeleteTimeout());
    Assert.assertEquals(funnyDuration, config.getHostUpdateTimeout());
    Assert.assertEquals(funnyDuration, config.getUpdateAllTimeout());
    Assert.assertEquals(funnyDuration.getMillis(), config.getPeriod());
    Assert.assertEquals(1200, config.getThreadPoolSize());
  }

  @Test
  public void testSimpleConfigDefaults()
  {
    final LookupCoordinatorManagerConfig config = new LookupCoordinatorManagerConfig();
    Assert.assertEquals(LookupCoordinatorManagerConfig.DEFAULT_DELETE_ALL_TIMEOUT, config.getDeleteAllTimeout());
    Assert.assertEquals(LookupCoordinatorManagerConfig.DEFAULT_HOST_DELETE_TIMEOUT, config.getHostDeleteTimeout());
    Assert.assertEquals(LookupCoordinatorManagerConfig.DEFAULT_HOST_UPDATE_TIMEOUT, config.getHostUpdateTimeout());
    Assert.assertEquals(LookupCoordinatorManagerConfig.DEFAULT_UPDATE_ALL_TIMEOUT, config.getUpdateAllTimeout());
    Assert.assertEquals(10, config.getThreadPoolSize());
    Assert.assertEquals(30_000, config.getPeriod());
  }
}
