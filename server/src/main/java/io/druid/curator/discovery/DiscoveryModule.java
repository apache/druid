/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.curator.discovery;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import io.druid.guice.DruidBinders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.KeyHolder;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.server.DruidNode;
import io.druid.server.initialization.CuratorDiscoveryConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableExecutorService;
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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

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

  /**
   * Requests that the un-annotated DruidNode instance be injected and published as part of the lifecycle.
   * 
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
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
   * Announcement will happen in the LAST stage of the Lifecycle
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
   * Announcement will happen in the LAST stage of the Lifecycle
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
   * Announcement will happen in the LAST stage of the Lifecycle
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
          public void start() throws Exception
          {
            if (nodes == null) {
              nodes = Lists.newArrayList();
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
        Lifecycle.Stage.LAST
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
              throw Throwables.propagate(e);
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
    public void start() throws Exception
    {

    }

    @Override
    public void registerService(ServiceInstance<T> service) throws Exception
    {

    }

    @Override
    public void updateService(ServiceInstance<T> service) throws Exception
    {

    }

    @Override
    public void unregisterService(ServiceInstance<T> service) throws Exception
    {

    }

    @Override
    public ServiceCacheBuilder<T> serviceCacheBuilder()
    {
      return new NoopServiceCacheBuilder<>();
    }

    @Override
    public Collection<String> queryForNames() throws Exception
    {
      return ImmutableList.of();
    }

    @Override
    public Collection<ServiceInstance<T>> queryForInstances(String name) throws Exception
    {
      return ImmutableList.of();
    }

    @Override
    public ServiceInstance<T> queryForInstance(String name, String id) throws Exception
    {
      return null;
    }

    @Override
    public ServiceProviderBuilder<T> serviceProviderBuilder()
    {
      return new NoopServiceProviderBuilder<>();
    }

    @Override
    public void close() throws IOException
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
      public void start() throws Exception
      {
        // nothing
      }

      @Override
      public void close() throws IOException
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
    public void start() throws Exception
    {
      // nothing
    }

    @Override
    public ServiceInstance<T> getInstance() throws Exception
    {
      return null;
    }

    @Override
    public Collection<ServiceInstance<T>> getAllInstances() throws Exception
    {
      return Collections.emptyList();
    }

    @Override
    public void noteError(ServiceInstance<T> tServiceInstance)
    {
      // nothing
    }

    @Override
    public void close() throws IOException
    {
      // nothing
    }
  }
}
