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

package org.apache.druid.guice;

import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.server.metrics.DefaultLoadSpecHolder;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class DefaultServerHolderModuleTest
{
  @Test
  public void testModuleLoadedForNonPeonRoles()
  {
    final Injector injector = makeInjector(Set.of());
    Assert.assertTrue(injector.getInstance(TaskHolder.class) instanceof NoopTaskHolder);
    Assert.assertTrue(injector.getInstance(LoadSpecHolder.class) instanceof DefaultLoadSpecHolder);
  }

  @Test
  public void testModuleExcludedForPeonRole()
  {
    final Injector injector = makeInjector(Set.of(NodeRole.PEON));
    Assert.assertThrows(
        "TaskHolder should not be bound for Peon servers",
        ConfigurationException.class,
        () -> injector.getInstance(TaskHolder.class)
    );

    Assert.assertThrows(
        "LoadSpecHolder should not be bound for Peon servers",
        ConfigurationException.class,
        () -> injector.getInstance(LoadSpecHolder.class)
    );
  }

  private Injector makeInjector(Set<NodeRole> nodeRoles)
  {
    return new CoreInjectorBuilder(new StartupInjectorBuilder().build(), nodeRoles)
        .addAll(List.of(new DefaultServerHolderModule()))
        .build();
  }
}
