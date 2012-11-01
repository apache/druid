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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ZKPhoneBook;
import com.metamx.druid.http.FileRequestLogger;
import com.metamx.druid.http.RequestLogger;
import com.metamx.druid.zk.StringZkSerializer;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProvider;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);

  private static volatile Properties props = null;

  public static ZkClient makeZkClient(ZkClientConfig config, Lifecycle lifecycle)
  {
    final ZkClient retVal = new ZkClient(
        new ZkConnection(config.getZkHosts()),
        config.getConnectionTimeout(),
        new StringZkSerializer()
    );

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            retVal.waitUntilConnected();
          }

          @Override
          public void stop()
          {
            retVal.close();
          }
        }
    );

    return retVal;
  }

  public static ZKPhoneBook createYellowPages(
      ObjectMapper jsonMapper, ZkClient zkClient, String threadNameFormat, Lifecycle lifecycle
  )
  {
    return lifecycle.addManagedInstance(
        new ZKPhoneBook(
            jsonMapper,
            zkClient,
            Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat(threadNameFormat)
                    .build()
            )
        )
    );
  }

  public static Properties loadProperties()
  {
    if (props != null) {
      return props;
    }

    Properties loadedProps = null;
    final InputStream stream = ClassLoader.getSystemResourceAsStream("runtime.properties");
    if (stream == null) {
      log.info("runtime.properties didn't exist as a resource, loading system properties instead.");
      loadedProps = System.getProperties();
    } else {
      log.info("Loading properties from runtime.properties.");
      try {
        loadedProps = new Properties();
        try {
          loadedProps.load(stream);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      finally {
        Closeables.closeQuietly(stream);
      }
    }

    for (String prop : loadedProps.stringPropertyNames()) {
      log.info("Loaded Property[%s] as [%s]", prop, loadedProps.getProperty(prop));
    }

    props = loadedProps;

    return loadedProps;
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

  public static CuratorFramework makeCuratorFrameworkClient(
      String zkHosts,
      Lifecycle lifecycle
  ) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .connectString(zkHosts)
                               .retryPolicy(new ExponentialBackoffRetry(1000, 30))
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

  public static ServiceDiscovery makeServiceDiscoveryClient(
      CuratorFramework discoveryClient,
      ServiceDiscoveryConfig config,
      Lifecycle lifecycle
  )
      throws Exception
  {
    final ServiceInstance serviceInstance =
        ServiceInstance.builder()
                       .name(config.getServiceName().replace('/', ':'))
                       .port(config.getPort())
                       .build();
    final ServiceDiscovery serviceDiscovery =
        ServiceDiscoveryBuilder.builder(Void.class)
                               .basePath(config.getDiscoveryPath())
                               .client(discoveryClient)
                               .thisInstance(serviceInstance)
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

  public static RequestLogger makeRequestLogger(ScheduledExecutorService exec, Properties props) throws IOException
  {
    final String property = "druid.request.logging.dir";
    final String loggingDir = props.getProperty(property);

    if (loggingDir == null) {
      throw new ISE("property[%s] not set.", property);
    }

    return new FileRequestLogger(exec, new File(loggingDir));
  }
}
