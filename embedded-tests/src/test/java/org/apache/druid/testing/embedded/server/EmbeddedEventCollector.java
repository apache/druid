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

package org.apache.druid.testing.embedded.server;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.testing.cli.CliEventCollector;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.docker.DruidContainerResource;
import org.apache.http.client.utils.URIBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link EmbeddedDruidServer} provided by the extension {@code testing-tools}.
 * This server collects events emitted using the {@code HttpPostEmitter}.
 */
public class EmbeddedEventCollector extends EmbeddedDruidServer<EmbeddedEventCollector>
{
  @Override
  protected ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new EventCollector(handler);
  }

  public String getMetricsUrl()
  {
    // Use the container-friendly hostname
    return new URIBuilder()
        .setScheme("http")
        .setHost(DruidContainerResource.getDefaultHost())
        .setPort(CliEventCollector.PORT)
        .setPath("/druid-ext/testing-tools/events")
        .toString();
  }

  private class EventCollector extends CliEventCollector
  {
    private final LifecycleInitHandler handler;

    private EventCollector(LifecycleInitHandler handler)
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
      modules.add(EmbeddedEventCollector.this::bindReferenceHolder);
      return modules;
    }
  }
}
