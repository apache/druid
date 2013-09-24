/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.guice.DruidBinders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.KeyHolder;
import io.druid.guice.LazySingleton;
import io.druid.server.DruidNode;
import io.druid.server.initialization.CuratorDiscoveryConfig;
import org.apache.curator.framework.CuratorFramework;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * The DiscoveryModule allows for the registration of Keys of DruidNode objects, which it intends to be
 * automatically announced at the end of the lifecycle start.
 * <p/>
 * In order for this to work a ServiceAnnouncer instance *must* be injected and instantiated first.
 * This can often be achieved by registering ServiceAnnouncer.class with the LifecycleModule.
 */
public class DiscoveryModule implements Module
{
  private static final String NAME = "DiscoveryModule:internal";

  /**
   * Requests that the un-annotated DruidNode instance be injected and published as part of the lifecycle.
   * <p/>
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   */
  public static void registerDefault(Binder binder)
  {
    registerKey(binder, Key.get(new TypeLiteral<DruidNode>(){}));
  }

  /**
   * Requests that the annotated DruidNode instance be injected and published as part of the lifecycle.
   * <p/>
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
   * <p/>
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @param annotation The annotation class to use in finding the DruidNode instance
   */
  public static void register(Binder binder, Class<? extends Annotation> annotation)
  {
    registerKey(binder, Key.get(new TypeLiteral<DruidNode>(){}, annotation));
  }

  /**
   * Requests that the keyed DruidNode instance be injected and published as part of the lifecycle.
   * <p/>
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @param key The key to use in finding the DruidNode instance
   */
  public static void registerKey(Binder binder, Key<DruidNode> key)
  {
    DruidBinders.discoveryAnnouncementBinder(binder).addBinding().toInstance(new KeyHolder<>(key));
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.discovery.curator", CuratorDiscoveryConfig.class);

    binder.bind(CuratorServiceAnnouncer.class).in(LazySingleton.class);

    // Build the binder so that it will at a minimum inject an empty set.
    DruidBinders.discoveryAnnouncementBinder(binder);

    // We bind this eagerly so that it gets instantiated and registers stuff with Lifecycle as a side-effect
    binder.bind(ServiceAnnouncer.class)
          .to(Key.get(CuratorServiceAnnouncer.class, Names.named(NAME)))
          .asEagerSingleton();
  }

  @Provides
  @LazySingleton
  @Named(NAME)
  public CuratorServiceAnnouncer getServiceAnnouncer(
      final CuratorServiceAnnouncer announcer,
      final Injector injector,
      final Set<KeyHolder<DruidNode>> nodesToAnnounce,
      final Lifecycle lifecycle
  )
  {
    lifecycle.addHandler(
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

    lifecycle.addHandler(
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

      }

      @Override
      public void close() throws IOException
      {

      }

      @Override
      public void addListener(ServiceCacheListener listener)
      {

      }

      @Override
      public void addListener(
          ServiceCacheListener listener, Executor executor
      )
      {

      }

      @Override
      public void removeListener(ServiceCacheListener listener)
      {

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
    public ServiceProviderBuilder<T> refreshPaddingMs(int refreshPaddingMs)
    {
      return this;
    }
  }

  private static class NoopServiceProvider<T> implements ServiceProvider<T>
  {
    @Override
    public void start() throws Exception
    {

    }

    @Override
    public ServiceInstance<T> getInstance() throws Exception
    {
      return null;
    }

    @Override
    public void close() throws IOException
    {

    }
  }
}
