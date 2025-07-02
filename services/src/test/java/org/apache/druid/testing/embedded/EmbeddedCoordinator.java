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

package org.apache.druid.testing.embedded;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliCoordinator;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.List;

/**
 * Embedded mode of {@link CliCoordinator} used in embedded tests.
 * Add this to your {@link EmbeddedDruidCluster} if you want to assign segments
 * to historicals.
 */
public class EmbeddedCoordinator extends EmbeddedDruidServer
{
  public EmbeddedCoordinator()
  {
    addProperty("druid.coordinator.startDelay", "PT0.1S");
    addProperty("druid.coordinator.period", "PT0.5S");
    addProperty("druid.manager.segments.pollDuration", "PT0.1S");
  }

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Coordinator(handler);
  }

  private class Coordinator extends CliCoordinator
  {
    private final LifecycleInitHandler handler;

    private Coordinator(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(super.getModules());
      modules.add(EmbeddedCoordinator.this::bindReferenceHolder);
      return modules;
    }
  }
}
