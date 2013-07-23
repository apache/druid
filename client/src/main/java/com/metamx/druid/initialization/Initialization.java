/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorConfig;
import com.metamx.druid.curator.PotentiallyGzippedCompressionProvider;
import com.metamx.druid.curator.discovery.AddressPortServiceInstanceFactory;
import com.metamx.druid.curator.discovery.CuratorServiceAnnouncer;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.curator.discovery.ServiceInstanceFactory;
import com.metamx.druid.guice.DruidGuiceExtensions;
import com.metamx.druid.guice.DruidSecondaryModule;
import com.metamx.druid.http.EmittingRequestLogger;
import com.metamx.druid.http.FileRequestLogger;
import com.metamx.druid.http.RequestLogger;
import com.metamx.druid.jackson.JacksonModule;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.core.Emitter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.zookeeper.data.Stat;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.skife.config.ConfigurationObjectFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);

  private static final String PROPERTIES_FILE = "runtime.properties";
  private static final Properties zkProps = new Properties();
  private static final Properties fileProps = new Properties(zkProps);
  private static Properties props = null;

  /**
   * Load properties.
   * Properties are layered:
   *
   * # stored in zookeeper
   * # runtime.properties file,
   * # cmdLine -D
   *
   * command line overrides runtime.properties which overrides zookeeper
   *
   * Idempotent. Thread-safe.  Properties are only loaded once.
   * If property druid.zk.service.host is not set then do not load properties from zookeeper.
   *
   * @return Properties ready to use.
   */
  public synchronized static Properties loadProperties()
  {
    if (props != null) {
      return props;
    }

    // Note that zookeeper coordinates must be either in cmdLine or in runtime.properties
    Properties sp = System.getProperties();

    Properties tmp_props = new Properties(fileProps); // the head of the 3 level Properties chain
    tmp_props.putAll(sp);

    final InputStream stream = ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE);
    if (stream == null) {
      log.info("%s not found on classpath, relying only on system properties and zookeeper.", PROPERTIES_FILE);
    } else {
      log.info("Loading properties from %s", PROPERTIES_FILE);
      try {
        try {
          fileProps.load(stream);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      finally {
        Closeables.closeQuietly(stream);
      }
    }

    // log properties from file; stringPropertyNames() would normally cascade down into the sub Properties objects, but
    //    zkProps (the parent level) is empty at this point so it will only log properties from runtime.properties
    for (String prop : fileProps.stringPropertyNames()) {
      log.info("Loaded(runtime.properties) Property[%s] as [%s]", prop, fileProps.getProperty(prop));
    }

    final String zkHostsProperty = "druid.zk.service.host";

    if (tmp_props.getProperty(zkHostsProperty) != null) {
      final ConfigurationObjectFactory factory = Config.createFactory(tmp_props);

      ZkPathsConfig config;
      try {
        config = factory.build(ZkPathsConfig.class);
      }
      catch (IllegalArgumentException e) {
        log.warn(e, "Unable to build ZkPathsConfig.  Cannot load properties from ZK.");
        config = null;
      }

      if (config != null) {
        Lifecycle lifecycle = new Lifecycle();
        try {
          CuratorFramework curator = makeCuratorFramework(factory.build(CuratorConfig.class), lifecycle);

          lifecycle.start();

          final Stat stat = curator.checkExists().forPath(config.getPropertiesPath());
          if (stat != null) {
            final byte[] data = curator.getData().forPath(config.getPropertiesPath());
            zkProps.load(new InputStreamReader(new ByteArrayInputStream(data), Charsets.UTF_8));
          }

          // log properties from zk
          for (String prop : zkProps.stringPropertyNames()) {
            log.info("Loaded(zk) Property[%s] as [%s]", prop, zkProps.getProperty(prop));
          }
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        finally {
          lifecycle.stop();
        }
      }
    } else {
      log.warn("property[%s] not set, skipping ZK-specified properties.", zkHostsProperty);
    }

    props = tmp_props;

    return props;
  }

  public static Server makeJettyServer(ServerConfig config)
  {
    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMinThreads(config.getNumThreads());
    threadPool.setMaxThreads(config.getNumThreads());

    final Server server = new Server();
    server.setThreadPool(threadPool);

    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(config.getPort());
    connector.setMaxIdleTime(config.getMaxIdleTimeMillis());
    connector.setStatsOn(true);

    server.setConnectors(new Connector[]{connector});

    return server;
  }

  public static CuratorFramework makeCuratorFramework(
      CuratorConfig curatorConfig,
      Lifecycle lifecycle
  ) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .connectString(curatorConfig.getZkHosts())
                               .sessionTimeoutMs(curatorConfig.getZkSessionTimeoutMs())
                               .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
                               // Don't compress stuff written just yet, need to get code deployed first.
                               .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                               .build();

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            framework.start();
          }

          @Override
          public void stop()
          {
            framework.close();
          }
        }
    );

    return framework;
  }

  public static ServiceDiscovery<Void> makeServiceDiscoveryClient(
      CuratorFramework discoveryClient,
      CuratorDiscoveryConfig config,
      Lifecycle lifecycle
  )
      throws Exception
  {
    final ServiceDiscovery<Void> serviceDiscovery =
        ServiceDiscoveryBuilder.builder(Void.class)
                               .basePath(config.getPath())
                               .client(discoveryClient)
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

  public static ServiceAnnouncer makeServiceAnnouncer(
      DruidNodeConfig config,
      ServiceDiscovery<Void> serviceDiscovery
  )
  {
    final ServiceInstanceFactory<Void> serviceInstanceFactory = makeServiceInstanceFactory(config);
    return new CuratorServiceAnnouncer(serviceDiscovery, serviceInstanceFactory);
  }

  public static void announceDefaultService(
      final DruidNodeConfig nodeConfig,
      final ServiceAnnouncer serviceAnnouncer,
      final Lifecycle lifecycle
  ) throws Exception
  {
    final String service = nodeConfig.getServiceName().replace('/', ':');

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            serviceAnnouncer.announce(service);
          }

          @Override
          public void stop()
          {
            try {
              serviceAnnouncer.unannounce(service);
            }
            catch (Exception e) {
              log.warn(e, "Failed to unannouce default service[%s]", service);
            }
          }
        }
    );
  }

  public static ServiceProvider makeServiceProvider(
      String serviceName,
      ServiceDiscovery serviceDiscovery,
      Lifecycle lifecycle
  )
  {
    final ServiceProvider serviceProvider = serviceDiscovery.serviceProviderBuilder()
                                                            .serviceName(serviceName)
                                                            .build();

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            serviceProvider.start();
          }

          @Override
          public void stop()
          {
            try {
              serviceProvider.close();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    return serviceProvider;
  }

  public static RequestLogger makeFileRequestLogger(
    ObjectMapper objectMapper,
    ScheduledExecutorFactory factory,
    Properties props
  ) throws IOException
  {
    return new FileRequestLogger(
        objectMapper,
        factory.create(1, "RequestLogger-%s"),
        new File(PropUtils.getProperty(props, "druid.request.logging.dir"))
    );
  }

  public static RequestLogger makeEmittingRequestLogger(Properties props, Emitter emitter)
  {
    return new EmittingRequestLogger(
        PropUtils.getProperty(props, "druid.service"),
        PropUtils.getProperty(props, "druid.host"),
        emitter,
        PropUtils.getProperty(props, "druid.request.logging.feed")
    );
  }

  public static ServiceInstanceFactory<Void> makeServiceInstanceFactory(DruidNodeConfig config)
  {
    final String host = config.getHost();
    final String address;
    final int colon = host.indexOf(':');
    if (colon < 0) {
      address = host;
    } else {
      address = host.substring(0, colon);
    }

    return new AddressPortServiceInstanceFactory(address, config.getPort());
  }

  public static Injector makeInjector(final Object... modules)
  {
    final Injector baseInjector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule("runtime.properties"),
        new ConfigModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);

            for (Object module : modules) {
              if (module instanceof Class) {
                binder.bind((Class) module);
              }
            }
          }
        }
    );

    List<Object> actualModules = Lists.newArrayList();

    actualModules.add(DruidSecondaryModule.class);
    actualModules.addAll(Arrays.asList(modules));

    return Guice.createInjector(
        Lists.transform(
            actualModules,
            new Function<Object, Module>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Module apply(@Nullable Object input)
              {
                if (input instanceof Module) {
                  baseInjector.injectMembers(input);
                  return (Module) input;
                }
                if (input instanceof Class) {
                  if (Module.class.isAssignableFrom((Class) input)) {
                    return baseInjector.getInstance((Class<? extends Module>) input);
                  }
                  else {
                    throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
                  }
                }
                throw new ISE("Unknown module type[%s]", input.getClass());
              }
            }
        )
    );
  }
}
