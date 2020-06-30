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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;

import java.lang.annotation.Annotation;
import java.util.List;

/**
 *
 */
public abstract class ServerRunnable extends GuiceRunnable
{
  private static final EmittingLogger log = new EmittingLogger(ServerRunnable.class);

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
      throw new RuntimeException(e);
    }
  }

  public static void bindNodeRoleAndAnnouncer(Binder binder, DiscoverySideEffectsProvider discoverySideEffectsProvider)
  {
    Multibinder<NodeRole> selfBinder = Multibinder.newSetBinder(binder, NodeRole.class, Self.class);
    selfBinder.addBinding().toInstance(discoverySideEffectsProvider.nodeRole);

    bindAnnouncer(
        binder,
        discoverySideEffectsProvider
    );
  }

  public static void bindNodeRoleAndAnnouncer(
      Binder binder,
      Class<? extends Annotation> annotation,
      DiscoverySideEffectsProvider discoverySideEffectsProvider
  )
  {
    Multibinder<NodeRole> selfBinder = Multibinder.newSetBinder(binder, NodeRole.class, Self.class);
    selfBinder.addBinding().toInstance(discoverySideEffectsProvider.nodeRole);

    bindAnnouncer(
        binder,
        annotation,
        discoverySideEffectsProvider
    );
  }

  private static void bindAnnouncer(
      final Binder binder,
      final DiscoverySideEffectsProvider provider
  )
  {
    binder.bind(DiscoverySideEffectsProvider.Child.class)
          .toProvider(provider)
          .in(LazySingleton.class);

    LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class));
  }

  private static void bindAnnouncer(
      final Binder binder,
      final Class<? extends Annotation> annotation,
      final DiscoverySideEffectsProvider provider
  )
  {
    binder.bind(DiscoverySideEffectsProvider.Child.class)
          .annotatedWith(annotation)
          .toProvider(provider)
          .in(LazySingleton.class);

    LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class, annotation));
  }

  /**
   * This is a helper class used by CliXXX classes to announce {@link DiscoveryDruidNode}
   * as part of {@link Lifecycle.Stage#ANNOUNCEMENTS}.
   */
  protected static class DiscoverySideEffectsProvider implements Provider<DiscoverySideEffectsProvider.Child>
  {
    public static class Child
    {
    }

    public static class Builder
    {
      private NodeRole nodeRole;
      private List<Class<? extends DruidService>> serviceClasses = ImmutableList.of();
      private boolean useLegacyAnnouncer;

      public Builder(final NodeRole nodeRole)
      {
        this.nodeRole = nodeRole;
      }

      public Builder serviceClasses(final List<Class<? extends DruidService>> serviceClasses)
      {
        this.serviceClasses = serviceClasses;
        return this;
      }

      public Builder useLegacyAnnouncer(final boolean useLegacyAnnouncer)
      {
        this.useLegacyAnnouncer = useLegacyAnnouncer;
        return this;
      }

      public DiscoverySideEffectsProvider build()
      {
        return new DiscoverySideEffectsProvider(
            nodeRole,
            serviceClasses,
            useLegacyAnnouncer
        );
      }
    }

    public static Builder builder(final NodeRole nodeRole)
    {
      return new Builder(nodeRole);
    }

    @Inject
    @Self
    private DruidNode druidNode;

    @Inject
    private DruidNodeAnnouncer announcer;

    @Inject
    private ServiceAnnouncer legacyAnnouncer;

    @Inject
    private Lifecycle lifecycle;

    @Inject
    private Injector injector;

    private final NodeRole nodeRole;
    private final List<Class<? extends DruidService>> serviceClasses;
    private final boolean useLegacyAnnouncer;

    private DiscoverySideEffectsProvider(
        final NodeRole nodeRole,
        final List<Class<? extends DruidService>> serviceClasses,
        final boolean useLegacyAnnouncer
    )
    {
      this.nodeRole = nodeRole;
      this.serviceClasses = serviceClasses;
      this.useLegacyAnnouncer = useLegacyAnnouncer;
    }

    @VisibleForTesting
    DiscoverySideEffectsProvider(
        final NodeRole nodeRole,
        final List<Class<? extends DruidService>> serviceClasses,
        final boolean useLegacyAnnouncer,
        final DruidNode druidNode,
        final DruidNodeAnnouncer announcer,
        final ServiceAnnouncer legacyAnnouncer,
        final Lifecycle lifecycle,
        final Injector injector
    )
    {
      this.nodeRole = nodeRole;
      this.serviceClasses = serviceClasses;
      this.useLegacyAnnouncer = useLegacyAnnouncer;
      this.druidNode = druidNode;
      this.announcer = announcer;
      this.legacyAnnouncer = legacyAnnouncer;
      this.lifecycle = lifecycle;
      this.injector = injector;
    }

    @Override
    public Child get()
    {
      ImmutableMap.Builder<String, DruidService> builder = new ImmutableMap.Builder<>();
      for (Class<? extends DruidService> clazz : serviceClasses) {
        DruidService service = injector.getInstance(clazz);
        if (service.isDiscoverable()) {
          builder.put(service.getName(), service);
        } else {
          log.info(
              "Service[%s] is not discoverable. This will not be listed as a service provided by this node.",
              service.getName()
          );
        }
      }
      DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(druidNode, nodeRole, builder.build());

      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              announcer.announce(discoveryDruidNode);

              if (useLegacyAnnouncer) {
                legacyAnnouncer.announce(discoveryDruidNode.getDruidNode());
              }
            }

            @Override
            public void stop()
            {
              // Reverse order vs. start().

              if (useLegacyAnnouncer) {
                legacyAnnouncer.unannounce(discoveryDruidNode.getDruidNode());
              }

              announcer.unannounce(discoveryDruidNode);
            }
          },
          Lifecycle.Stage.ANNOUNCEMENTS
      );
      return new Child();
    }
  }
}
