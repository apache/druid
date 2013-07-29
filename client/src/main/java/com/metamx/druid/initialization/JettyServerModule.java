package com.metamx.druid.initialization;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.guice.ConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;
import org.eclipse.jetty.server.Server;

import javax.servlet.ServletException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JettyServerModule extends JerseyServletModule
{
  private static final Logger log = new Logger(JettyServerModule.class);

  private final JettyServerInitializer initializer;
  private final List<Class<?>> resources = Lists.newArrayList();

  public JettyServerModule(
      JettyServerInitializer initializer
  )
  {
    this.initializer = initializer;
  }

  public JettyServerModule addResource(Class<?> resource)
  {
    resources.add(resource);
    return this;
  }

  @Override
  protected void configureServlets()
  {
    Binder binder = binder();

    ConfigProvider.bind(binder, ServerConfig.class);

    // The Guice servlet extension doesn't actually like requiring explicit bindings, so we do its job for it here.
    try {
      final Class<?> classToBind = Class.forName(
          "com.google.inject.servlet.InternalServletModule$BackwardsCompatibleServletContextProvider"
      );
      binder.bind(classToBind);
    }
    catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }

    binder.bind(GuiceContainer.class).to(DruidGuiceContainer.class);
    binder.bind(DruidGuiceContainer.class).in(Scopes.SINGLETON);
    serve("/*").with(DruidGuiceContainer.class);

    final ImmutableSet<Class<?>> theResources = ImmutableSet.copyOf(resources);
    binder.bind(new TypeLiteral<Set<Class<?>>>(){})
          .annotatedWith(Names.named("resourceClasses"))
          .toInstance(theResources);
    for (Class<?> resource : theResources) {
      binder.bind(resource);
    }

    binder.bind(Key.get(Server.class, Names.named("ForTheEagerness"))).to(Server.class).asEagerSingleton();
  }

  public static class DruidGuiceContainer extends GuiceContainer
  {
    private final Set<Class<?>> resources;

    @Inject
    public DruidGuiceContainer(
        Injector injector,
        @Named("resourceClasses") Set<Class<?>> resources
    )
    {
      super(injector);
      this.resources = resources;
    }

    @Override
    protected ResourceConfig getDefaultResourceConfig(
        Map<String, Object> props, WebConfig webConfig
    ) throws ServletException
    {
      return new DefaultResourceConfig(resources);
    }
  }

  @Provides @LazySingleton
  public Server getServer(Injector injector, Lifecycle lifecycle, ServerConfig config)
  {
    final Server server = Initialization.makeJettyServer(config);
    initializer.initialize(server, injector);

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            server.start();
          }

          @Override
          public void stop()
          {
            try {
              server.stop();
            }
            catch (Exception e) {
              log.warn(e, "Unable to stop Jetty server.");
            }
          }
        }
    );
    return server;
  }
}
