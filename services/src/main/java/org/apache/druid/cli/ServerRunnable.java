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

package org.apache.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;

import java.util.List;

/**
 */
public abstract class ServerRunnable extends GuiceRunnable
{
  public ServerRunnable(Logger log)
  {
    super(log);
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final Lifecycle lifecycle = initLifecycle(injector);

    try {
      lifecycle.join();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * This is a helper class used by CliXXX classes to announce DiscoveryDruidNode
   * as part of lifecycle Stage.LAST .
   */
  protected static class DiscoverySideEffectsProvider implements Provider<DiscoverySideEffectsProvider.Child>
  {
    public static class Child {}

    @Inject @Self
    private DruidNode druidNode;

    @Inject
    private DruidNodeAnnouncer announcer;

    @Inject
    private Lifecycle lifecycle;

    @Inject
    private Injector injector;

    private final String nodeType;
    private final List<Class<? extends DruidService>> serviceClasses;

    public DiscoverySideEffectsProvider(String nodeType, List<Class<? extends DruidService>> serviceClasses)
    {
      this.nodeType = nodeType;
      this.serviceClasses = serviceClasses;
    }

    @Override
    public Child get()
    {
      ImmutableMap.Builder<String, DruidService> builder = new ImmutableMap.Builder<>();
      for (Class<? extends DruidService> clazz : serviceClasses) {
        DruidService service = injector.getInstance(clazz);
        builder.put(service.getName(), service);
      }

      DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(druidNode,
                                                                     nodeType,
                                                                     builder.build()
      );

      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              announcer.announce(discoveryDruidNode);
            }

            @Override
            public void stop()
            {
              announcer.unannounce(discoveryDruidNode);
            }
          },
          Lifecycle.Stage.LAST
      );

      return new Child();
    }
  }
}
