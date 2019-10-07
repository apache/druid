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

package org.apache.druid.curator.discovery;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceCacheBuilder;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.ServiceProviderBuilder;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.KeyHolder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.CuratorDiscoveryConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

/**
 * The DiscoveryModule allows for the registration of Keys of DruidNode objects, which it intends to be
 * automatically announced at the end of the lifecycle start.
 * 
 * In order for this to work a ServiceAnnouncer instance *must* be injected and instantiated first.
 * This can often be achieved by registering ServiceAnnouncer.class with the LifecycleModule.
 */
public class DiscoveryModule implements Module
{
  private static final String NAME = "DiscoveryModule:internal";

  private static final String INTERNAL_DISCOVERY_PROP = "druid.discovery.type";
  private static final String CURATOR_KEY = "curator";

  /**
   * Requests that the un-annotated DruidNode instance be injected and published as part of the lifecycle.
   * 
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class)) automatically.
   * Announcement will happen in the ANNOUNCEMENTS stage of the Lifecycle
   *
   * @param binder the Binder to register with
   */
  public static void registerDefault(Binder binder)
  {
    registerKey(binder, Key.get(new TypeLiteral<DruidNode>(){}));
  }

  /**
   * Requests that the annotated DruidNode instance be injected and published as part of the lifecycle.
   * 
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the ANNOUNCEMENTS stage of the Lifecycle
   *
   * @param annotation The annotation instance to use in finding the DruidNode instance, usually a Named annotation
   */
  public static void register(Binder binder, Annotation annotation)
  {
    registerKey(binder, Key.get(new TypeLiteral<DruidNode>(){}, annotation));
  }

  /**
   * Requests that the annotated DruidNode instance be injected and published as part of the lifecycle.
   * 
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the ANNOUNCEMENTS stage of the Lifecycle
   *
   * @param binder the Binder to register with
   * @param annotation The annotation class to use in finding the DruidNode instance
   */
  public static void register(Binder binder, Class<? extends Annotation> annotation)
  {
    registerKey(binder, Key.get(new TypeLiteral<DruidNode>(){}, annotation));
  }

  /**
   * Requests that the keyed DruidNode instance be injected and published as part of the lifecycle.
   * 
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the ANNOUNCEMENTS stage of the Lifecycle
   *
   * @param binder the Binder to register with
   * @param key The key to use in finding the DruidNode instance
   */
  public static void registerKey(Binder binder, Key<DruidNode> key)
  {
    DruidBinders.discoveryAnnouncementBinder(binder).addBinding().toInstance(new KeyHolder<>(key));
    LifecycleModule.register(binder, ServiceAnnouncer.class);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.discovery.curator", CuratorDiscoveryConfig.class);

    binder.bind(CuratorServiceAnnouncer.class).in(LazySingleton.class);

    // Build the binder so that it will at a minimum inject an empty set.
    DruidBinders.discoveryAnnouncementBinder(binder);

    binder.bind(ServiceAnnouncer.class)
          .to(Key.get(CuratorServiceAnnouncer.class, Names.named(NAME)))
          .in(LazySingleton.class);

    // internal discovery bindings.
    PolyBind.createChoiceWithDefault(binder, INTERNAL_DISCOVERY_PROP, Key.get(DruidNodeAnnouncer.class), CURATOR_KEY);

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidNodeDiscoveryProvider.class),
        CURATOR_KEY
    );

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidLeaderSelector.class, Coordinator.class),
        CURATOR_KEY
    );

    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidLeaderSelector.class, IndexingService.class),
        CURATOR_KEY
    );

    PolyBind.optionBinder(binder, Key.get(DruidNodeDiscoveryProvider.class))
            .addBinding(CURATOR_KEY)
            .to(CuratorDruidNodeDiscoveryProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeAnnouncer.class))
            .addBinding(CURATOR_KEY)
            .to(CuratorDruidNodeAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, Coordinator.class))
            .addBinding(CURATOR_KEY)
            .toProvider(new DruidLeaderSelectorProvider(
                (zkPathsConfig) -> ZKPaths.makePath(zkPathsConfig.getCoordinatorPath(), "_COORDINATOR"))
            )
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, IndexingService.class))
            .addBinding(CURATOR_KEY)
            .toProvider(
                new DruidLeaderSelectorProvider(
                    (zkPathsConfig) -> ZKPaths.makePath(zkPathsConfig.getOverlordPath(), "_OVERLORD")
                )
            )
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  @Named(NAME)
  public CuratorServiceAnnouncer getServiceAnnouncer(
      final CuratorServiceAnnouncer announcer,
      final Injector injector,
      final Set<KeyHolder<DruidNode>> nodesToAnnounce,
      final Lifecycle lifecycle
  ) throws Exception
  {
    lifecycle.addMaybeStartHandler(
        new Lifecycle.Handler()
        {
          private volatile List<DruidNode> nodes = null;

          @Override
          public void start()
          {
            if (nodes == null) {
              nodes = new ArrayList<>();
              for (KeyHolder<DruidNode> holder : nodesToAnnounce) {
                nodes.add(injector.getInstance(holder.getKey()));
              }
            }

            for (DruidNode node : nodes) {
              announcer.announce(node);
            }
          }

          @Override
          public void stop()
          {
            if (nodes != null) {
              for (DruidNode node : nodes) {
                announcer.unannounce(node);
              }
            }
          }
        },
        Lifecycle.Stage.ANNOUNCEMENTS
    );

    return announcer;
  }

  @Provides
  @LazySingleton
  public ServiceDiscovery<Void> getServiceDiscovery(
      CuratorFramework curator,
      CuratorDiscoveryConfig config,
      Lifecycle lifecycle
  ) throws Exception
  {
    if (!config.useDiscovery()) {
      return new NoopServiceDiscovery<>();
    }

    final ServiceDiscovery<Void> serviceDiscovery =
        ServiceDiscoveryBuilder.builder(Void.class)
                               .basePath(config.getPath())
                               .client(curator)
                               .build();

    lifecycle.addMaybeStartHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            serviceDiscovery.start();
          }

          @Override
          public void stop()
          {
            try {
              serviceDiscovery.close();
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    return serviceDiscovery;
  }

  @Provides
  @LazySingleton
  public ServerDiscoveryFactory getServerDiscoveryFactory(
      ServiceDiscovery<Void> serviceDiscovery
  )
  {
    return new ServerDiscoveryFactory(serviceDiscovery);
  }

  private static class NoopServiceDiscovery<T> implements ServiceDiscovery<T>
  {
    @Override
    public void start()
    {

    }

    @Override
    public void registerService(ServiceInstance<T> service)
    {

    }

    @Override
    public void updateService(ServiceInstance<T> service)
    {

    }

    @Override
    public void unregisterService(ServiceInstance<T> service)
    {

    }

    @Override
    public ServiceCacheBuilder<T> serviceCacheBuilder()
    {
      return new NoopServiceCacheBuilder<>();
    }

    @Override
    public Collection<String> queryForNames()
    {
      return ImmutableList.of();
    }

    @Override
    public Collection<ServiceInstance<T>> queryForInstances(String name)
    {
      return ImmutableList.of();
    }

    @Override
    public ServiceInstance<T> queryForInstance(String name, String id)
    {
      return null;
    }

    @Override
    public ServiceProviderBuilder<T> serviceProviderBuilder()
    {
      return new NoopServiceProviderBuilder<>();
    }

    @Override
    public void close()
    {

    }
  }

  private static class NoopServiceCacheBuilder<T> implements ServiceCacheBuilder<T>
  {
    @Override
    public ServiceCache<T> build()
    {
      return new NoopServiceCache<>();
    }

    @Override
    public ServiceCacheBuilder<T> name(String name)
    {
      return this;
    }

    @Override
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
      return this;
    }

    @Override
    public ServiceCacheBuilder<T> executorService(ExecutorService executorService)
    {
      return this;
    }

    @Override
    public ServiceCacheBuilder<T> executorService(CloseableExecutorService closeableExecutorService)
    {
      return this;
    }

    private static class NoopServiceCache<T> implements ServiceCache<T>
    {
      @Override
      public List<ServiceInstance<T>> getInstances()
      {
        return ImmutableList.of();
      }

      @Override
      public void start()
      {
        // nothing
      }

      @Override
      public void close()
      {
        // nothing
      }

      @Override
      public void addListener(ServiceCacheListener listener)
      {
        // nothing
      }

      @Override
      public void addListener(ServiceCacheListener listener, Executor executor)
      {
        // nothing
      }

      @Override
      public void removeListener(ServiceCacheListener listener)
      {
        // nothing
      }
    }
  }

  private static class NoopServiceProviderBuilder<T> implements ServiceProviderBuilder<T>
  {
    @Override
    public ServiceProvider<T> build()
    {
      return new NoopServiceProvider<>();
    }

    @Override
    public ServiceProviderBuilder<T> serviceName(String serviceName)
    {
      return this;
    }

    @Override
    public ServiceProviderBuilder<T> providerStrategy(ProviderStrategy<T> providerStrategy)
    {
      return this;
    }

    @Override
    public ServiceProviderBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
      return this;
    }

    @Override
    public ServiceProviderBuilder<T> downInstancePolicy(DownInstancePolicy downInstancePolicy)
    {
      return this;
    }

    @Override
    public ServiceProviderBuilder<T> additionalFilter(InstanceFilter<T> tInstanceFilter)
    {
      return this;
    }
  }

  private static class NoopServiceProvider<T> implements ServiceProvider<T>
  {
    @Override
    public void start()
    {
      // nothing
    }

    @Override
    public ServiceInstance<T> getInstance()
    {
      return null;
    }

    @Override
    public Collection<ServiceInstance<T>> getAllInstances()
    {
      return Collections.emptyList();
    }

    @Override
    public void noteError(ServiceInstance<T> tServiceInstance)
    {
      // nothing
    }

    @Override
    public void close()
    {
      // nothing
    }
  }

  private static class DruidLeaderSelectorProvider implements Provider<DruidLeaderSelector>
  {
    @Inject
    private CuratorFramework curatorFramework;

    @Inject
    @Self
    private DruidNode druidNode;

    @Inject
    private ZkPathsConfig zkPathsConfig;

    private final Function<ZkPathsConfig, String> latchPathFn;

    DruidLeaderSelectorProvider(Function<ZkPathsConfig, String> latchPathFn)
    {
      this.latchPathFn = latchPathFn;
    }

    @Override
    public DruidLeaderSelector get()
    {
      return new CuratorDruidLeaderSelector(
          curatorFramework,
          druidNode,
          latchPathFn.apply(zkPathsConfig)
      );
    }
  }
}
