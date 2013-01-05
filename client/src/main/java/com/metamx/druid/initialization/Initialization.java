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

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ZKPhoneBook;
import com.metamx.druid.http.FileRequestLogger;
import com.metamx.druid.http.RequestLogger;
import com.metamx.druid.utils.PropUtils;
import com.metamx.druid.zk.StringZkSerializer;
import com.metamx.druid.zk.PropertiesZkSerializer;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);

  private static final Properties zkProps = new Properties();
  private static final Properties fileProps = new Properties(zkProps);
  private static Properties props = null;
  public final static String PROP_SUBPATH = "properties";
  public final static String[] SUB_PATHS = {"announcements", "servedSegments", "loadQueue", "master"};
  public final static String[] SUB_PATH_PROPS = {
      "druid.zk.paths.announcementsPath",
      "druid.zk.paths.servedSegmentsPath",
      "druid.zk.paths.loadQueuePath",
      "druid.zk.paths.masterPath"
  };
  public static final String DEFAULT_ZPATH = "/druid";

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

  public static ZKPhoneBook createPhoneBook(
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


  /**
   * Load properties.
   * Properties are layered, high to low precedence:  cmdLine -D, runtime.properties file, stored in zookeeper.
   * Idempotent. Thread-safe.  Properties are only loaded once.
   * If property druid.zk.service.host=none then do not load properties from zookeeper.
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

    final InputStream stream = ClassLoader.getSystemResourceAsStream("runtime.properties");
    if (stream == null) {
      log.info(
          "runtime.properties not found as a resource in classpath, relying only on system properties, and zookeeper now."
      );
    } else {
      log.info("Loading properties from runtime.properties");
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

    // log properties from file; note stringPropertyNames() will follow Properties.defaults but
    //    next level is empty at this point.
    for (String prop : fileProps.stringPropertyNames()) {
      log.info("Loaded(runtime.properties) Property[%s] as [%s]", prop, fileProps.getProperty(prop));
    }

    final String zk_hosts = tmp_props.getProperty("druid.zk.service.host");

    if (zk_hosts != null) {
      if (!zk_hosts.equals("none")) { //  get props from zk
        final ZkClient zkPropLoadingClient;
        final ZkClientConfig clientConfig = new ZkClientConfig()
        {
          @Override
          public String getZkHosts()
          {
            return zk_hosts;
          }
        };

        zkPropLoadingClient = new ZkClient(
            new ZkConnection(clientConfig.getZkHosts()),
            clientConfig.getConnectionTimeout(),
            new PropertiesZkSerializer()
        );
        zkPropLoadingClient.waitUntilConnected();
        String propertiesZNodePath = tmp_props.getProperty("druid.zk.paths.propertiesPath");
        if (propertiesZNodePath == null) {
          String zpathBase = tmp_props.getProperty("druid.zk.paths.base", DEFAULT_ZPATH);
          propertiesZNodePath = makePropPath(zpathBase);
        }
        // get properties stored by zookeeper (lowest precedence)
        if (zkPropLoadingClient.exists(propertiesZNodePath)) {
          Properties p = zkPropLoadingClient.readData(propertiesZNodePath, true);
          if (p != null) {
            zkProps.putAll(p);
          }
        }
        // log properties from zk
        for (String prop : zkProps.stringPropertyNames()) {
          log.info("Loaded(properties stored in zk) Property[%s] as [%s]", prop, zkProps.getProperty(prop));
        }
      } // get props from zk
    } else {
      log.warn("property druid.zk.service.host is not set, so no way to contact zookeeper for coordination.");
    }
    // validate properties now that all levels of precedence are loaded
    if (!validateResolveProps(tmp_props)) {
      log.error("Properties failed to validate, cannot continue");
      throw new RuntimeException("Properties failed to validate");
    }
    props = tmp_props; // publish

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

  public static CuratorFramework makeCuratorFrameworkClient(
      CuratorConfig curatorConfig,
      Lifecycle lifecycle
  ) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .connectString(curatorConfig.getZkHosts())
                               .retryPolicy(
                                   new ExponentialBackoffRetry(
                                       1000,
                                       30
                                   )
                               )
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

  public static RequestLogger makeRequestLogger(ScheduledExecutorFactory factory, Properties props) throws IOException
  {
    return new FileRequestLogger(
        factory.create(1, "RequestLogger-%s"),
        new File(PropUtils.getProperty(props, "druid.request.logging.dir"))
    );
  }

  public static String makePropPath(String basePath)
  {
    return String.format("%s/%s", basePath, PROP_SUBPATH);
  }

  /**
   * Validate and Resolve Properties.
   * Resolve zpaths with props like druid.zk.paths.*Path using druid.zk.paths.base value.
   * Check validity so that if druid.zk.paths.*Path props are set, all are set,
   * if none set, then construct defaults relative to druid.zk.paths.base and add these
   * to the properties chain.
   *
   * @param props
   *
   * @return true if valid zpath properties.
   */
  public static boolean validateResolveProps(Properties props)
  {
    boolean zpathValidateFailed;//  validate druid.zk.paths.base
    String propertyZpath = props.getProperty("druid.zk.paths.base");
    zpathValidateFailed = zpathBaseCheck(propertyZpath, "property druid.zk.paths.base");

    String zpathEffective = DEFAULT_ZPATH;
    if (propertyZpath != null) {
      zpathEffective = propertyZpath;
    }

    final String propertiesZpathOverride = props.getProperty("druid.zk.paths.propertiesPath");

    if (!zpathValidateFailed) {
      System.out.println("Effective zpath prefix=" + zpathEffective);
    }

    //    validate druid.zk.paths.*Path properties
    //
    // if any zpath overrides are set in properties, they must start with /
    int zpathOverrideCount = 0;
    StringBuilder sbErrors = new StringBuilder(100);
    for (int i = 0; i < SUB_PATH_PROPS.length; i++) {
      String val = props.getProperty(SUB_PATH_PROPS[i]);
      if (val != null) {
        zpathOverrideCount++;
        if (!val.startsWith("/")) {
          sbErrors.append(SUB_PATH_PROPS[i]).append("=").append(val).append("\n");
          zpathValidateFailed = true;
        }
      }
    }
    // separately check druid.zk.paths.propertiesPath (not in SUB_PATH_PROPS since it is not a "dir")
    if (propertiesZpathOverride != null) {
      zpathOverrideCount++;
      if (!propertiesZpathOverride.startsWith("/")) {
        sbErrors.append("druid.zk.paths.propertiesPath").append("=").append(propertiesZpathOverride).append("\n");
        zpathValidateFailed = true;
      }
    }
    if (zpathOverrideCount == 0) {
      if (propertyZpath == null) { // if default base is used, store it as documentation
        props.setProperty("druid.zk.paths.base", zpathEffective);
      }
      //
      //   Resolve default zpaths using zpathEffective as base
      //
      for (int i = 0; i < SUB_PATH_PROPS.length; i++) {
        props.setProperty(SUB_PATH_PROPS[i], zpathEffective + "/" + SUB_PATHS[i]);
      }
      props.setProperty("druid.zk.paths.propertiesPath", zpathEffective + "/properties");
    }

    if (zpathValidateFailed) {
      System.err.println(
          "When overriding zk zpaths, with properties like druid.zk.paths.*Path " +
          "the znode path must start with '/' (slash) ; problem overrides:"
      );
      System.err.print(sbErrors.toString());
    }

    return !zpathValidateFailed;
  }

  /**
   * Check znode zpath base for proper slash, no trailing slash.
   *
   * @param zpathBase      znode base path, if null then this method does nothing.
   * @param errorMsgPrefix error context to use if errors are emitted, should indicate
   *                       where the zpathBase value came from.
   *
   * @return true if validate failed.
   */
  public static boolean zpathBaseCheck(String zpathBase, String errorMsgPrefix)
  {
    boolean zpathValidateFailed = false;
    if (zpathBase != null) {
      if (!zpathBase.startsWith("/")) {
        zpathValidateFailed = true;
        System.err.println(errorMsgPrefix + " must start with '/' (slash); found=" + zpathBase);
      }
      if (zpathBase.endsWith("/")) {
        zpathValidateFailed = true;
        System.err.println(errorMsgPrefix + " must NOT end with '/' (slash); found=" + zpathBase);
      }
    }
    return zpathValidateFailed;
  }
}
