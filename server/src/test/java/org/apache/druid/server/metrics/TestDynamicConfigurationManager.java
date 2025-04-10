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

package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DynamicConfigurationManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import java.util.Set;

public class TestDynamicConfigurationManager extends DynamicConfigurationManager
{
  public TestDynamicConfigurationManager()
  {
    // TODO: finish and add tests.
    super(null);
  }

  @Override
  public CoordinatorDynamicConfig getConfig()
  {
    return null;
  }

  @Override
  public Set<String> getTargetCloneServers()
  {
    return ImmutableSet.of();
  }

  @Override
  public Set<String> getSourceClusterServers()
  {
    return ImmutableSet.of();
  }

  @Override
  public void updateCloneServers(CoordinatorDynamicConfig updatedConfig)
  {
  }

  @Override
  public void start() throws InterruptedException
  {
  }
}
