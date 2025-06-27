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
import org.apache.druid.cli.CliMiddleManager;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.List;

/**
 * Embeddded mode of {@link CliMiddleManager} used in embedded tests.
 * Add this to your {@link EmbeddedDruidCluster} if you want to launch ingestion
 * tasks as child processes.
 *
 * @deprecated Use {@link EmbeddedIndexer} instead. {@link EmbeddedMiddleManager}
 * should be used only for local testing and never in committed embedded tests,
 * as it launches tasks as child processes which is undesirable in unit tests.
 */
@Deprecated
public class EmbeddedMiddleManager extends EmbeddedDruidServer
{

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new MiddleManager(handler);
  }

  private class MiddleManager extends CliMiddleManager
  {
    private final LifecycleInitHandler handler;

    private MiddleManager(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(super.getModules());
      modules.add(EmbeddedMiddleManager.this::bindReferenceHolder);
      return modules;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }
  }
}
