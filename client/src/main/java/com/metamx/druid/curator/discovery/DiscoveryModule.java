package com.metamx.druid.curator.discovery;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
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
import com.metamx.druid.guice.JsonConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.metamx.druid.initialization.CuratorDiscoveryConfig;
import com.metamx.druid.initialization.DruidNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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

  public final List<Key<Supplier<DruidNode>>> nodesToAnnounce = new CopyOnWriteArrayList<Key<Supplier<DruidNode>>>();
  public boolean configured = false;

  /**
   * Requests that the un-annotated DruidNode instance be injected and published as part of the lifecycle.
   *
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @return this, for chaining.
   */
  public DiscoveryModule registerDefault()
  {
    return registerKey(Key.get(new TypeLiteral<Supplier<DruidNode>>(){}));
  }

  /**
   * Requests that the annotated DruidNode instance be injected and published as part of the lifecycle.
   *
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @param annotation The annotation instance to use in finding the DruidNode instance, usually a Named annotation
   * @return this, for chaining.
   */
  public DiscoveryModule register(Annotation annotation)
  {
    return registerKey(Key.get(new TypeLiteral<Supplier<DruidNode>>(){}, annotation));
  }

  /**
   * Requests that the annotated DruidNode instance be injected and published as part of the lifecycle.
   *
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @param annotation The annotation class to use in finding the DruidNode instance
   * @return this, for chaining
   */
  public DiscoveryModule register(Class<? extends Annotation> annotation)
  {
    return registerKey(Key.get(new TypeLiteral<Supplier<DruidNode>>(){}, annotation));
  }

  /**
   * Requests that the keyed DruidNode instance be injected and published as part of the lifecycle.
   *
   * That is, this module will announce the DruidNode instance returned by
   * injector.getInstance(Key.get(DruidNode.class, annotation)) automatically.
   * Announcement will happen in the LAST stage of the Lifecycle
   *
   * @param key The key to use in finding the DruidNode instance
   * @return this, for chaining
   */
  public DiscoveryModule registerKey(Key<Supplier<DruidNode>> key)
  {
    synchronized (nodesToAnnounce) {
      Preconditions.checkState(!configured, "Cannot register key[%s] after configuration.", key);
    }
    nodesToAnnounce.add(key);
    return this;
  }

  @Override
  public void configure(Binder binder)
  {
    synchronized (nodesToAnnounce) {
      configured = true;
      JsonConfigProvider.bind(binder, "druid.discovery.curator", CuratorDiscoveryConfig.class);

      binder.bind(CuratorServiceAnnouncer.class).in(LazySingleton.class);

      // We bind this eagerly so that it gets instantiated and registers stuff with Lifecycle as a side-effect
      binder.bind(ServiceAnnouncer.class)
            .to(Key.get(CuratorServiceAnnouncer.class, Names.named(NAME)))
            .asEagerSingleton();
    }
  }

  @Provides @LazySingleton @Named(NAME)
  public CuratorServiceAnnouncer getServiceAnnouncer(
      final CuratorServiceAnnouncer announcer,
      final Injector injector,
      final Lifecycle lifecycle
  )
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          private volatile List<Supplier<DruidNode>> nodes = null;

          @Override
          public void start() throws Exception
          {
            if (nodes == null) {
              nodes = Lists.transform(
                  nodesToAnnounce,
                  new Function<Key<Supplier<DruidNode>>, Supplier<DruidNode>>()
                  {
                    @Nullable
                    @Override
                    public Supplier<DruidNode> apply(
                        @Nullable Key<Supplier<DruidNode>> input
                    )
                    {
                      return injector.getInstance(input);
                    }
                  }
              );
            }

            for (Supplier<DruidNode> node : nodes) {
              announcer.announce(node.get());
            }
          }

          @Override
          public void stop()
          {
            if (nodes != null) {
              for (Supplier<DruidNode> node : nodes) {
                announcer.unannounce(node.get());
              }
            }
          }
        },
        Lifecycle.Stage.LAST
    );

    return announcer;
  }

  @Provides @LazySingleton
  public ServiceDiscovery<Void> getServiceDiscovery(
      CuratorFramework curator,
      Supplier<CuratorDiscoveryConfig> config,
      Lifecycle lifecycle
  ) throws Exception
  {
    final ServiceDiscovery<Void> serviceDiscovery =
        ServiceDiscoveryBuilder.builder(Void.class)
                               .basePath(config.get().getPath())
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
        },
        Lifecycle.Stage.LAST
    );

    return serviceDiscovery;
  }
}
