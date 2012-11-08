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

package com.metamx.druid.http;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.ISE;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.BaseNode;
import com.metamx.druid.client.BrokerServerView;
import com.metamx.druid.client.CachingClusteredClient;
import com.metamx.druid.client.ClientConfig;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.cache.CacheBroker;
import com.metamx.druid.client.cache.CacheMonitor;
import com.metamx.druid.client.cache.MapCacheBroker;
import com.metamx.druid.client.cache.MapCacheBrokerConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.query.ReflectionQueryToolChestWarehouse;
import com.metamx.druid.utils.PropUtils;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.metrics.Monitor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.util.List;
import java.util.Properties;

/**
 */

public class BrokerNode extends BaseNode<BrokerNode>
{
  private static final Logger log = new Logger(BrokerNode.class);

  private final List<Module> extraModules = Lists.newArrayList();
  private final List<String> pathsForGuiceFilter = Lists.newArrayList();

  private QueryToolChestWarehouse warehouse = null;
  private HttpClient brokerHttpClient = null;
  private CacheBroker cacheBroker = null;

  private boolean useDiscovery = true;

  public static Builder builder()
  {
    return new Builder();
  }

  public BrokerNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }

  public QueryToolChestWarehouse getWarehouse()
  {
    initializeWarehouse();
    return warehouse;
  }

  public BrokerNode setWarehouse(QueryToolChestWarehouse warehouse)
  {
    checkFieldNotSetAndSet("warehouse", warehouse);
    return this;
  }

  public HttpClient getBrokerHttpClient()
  {
    initializeBrokerHttpClient();
    return brokerHttpClient;
  }

  public BrokerNode setBrokerHttpClient(HttpClient brokerHttpClient)
  {
    checkFieldNotSetAndSet("brokerHttpClient", brokerHttpClient);
    return this;
  }

  public CacheBroker getCacheBroker()
  {
    initializeCacheBroker();
    return cacheBroker;
  }

  public BrokerNode setCacheBroker(CacheBroker cacheBroker)
  {
    checkFieldNotSetAndSet("cacheBroker", cacheBroker);
    return this;
  }

  public BrokerNode useDiscovery(boolean useDiscovery)
  {
    this.useDiscovery = useDiscovery;
    return this;
  }

  /**
   * This method allows you to specify more Guice modules to use primarily for injected extra Jersey resources.
   * I'd like to remove the Guice dependency for this, but I don't know how to set up Jersey without Guice...
   *
   * This is deprecated because at some point in the future, we will eliminate the Guice dependency and anything
   * that uses this will break.  Use at your own risk.
   *
   * @param module the module to register with Guice
   *
   * @return this
   */
  @Deprecated
  public BrokerNode addModule(Module module)
  {
    extraModules.add(module);
    return this;
  }

  /**
   * This method is used to specify extra paths that the GuiceFilter should pay attention to.
   *
   * This is deprecated for the same reason that addModule is deprecated.
   *
   * @param path the path that the GuiceFilter should pay attention to.
   *
   * @return this
   */
  @Deprecated
  public BrokerNode addPathForGuiceFilter(String path)
  {
    pathsForGuiceFilter.add(path);
    return this;
  }

  @Override
  protected void doInit() throws Exception
  {
    initializeWarehouse();
    initializeBrokerHttpClient();
    initializeCacheBroker();
    initializeDiscovery();

    final Lifecycle lifecycle = getLifecycle();

    final List<Monitor> monitors = getMonitors();
    monitors.add(new CacheMonitor(cacheBroker));
    startMonitoring(monitors);

    final BrokerServerView view = new BrokerServerView(warehouse, getSmileMapper(), brokerHttpClient);
    final ClientInventoryManager clientInventoryManager = new ClientInventoryManager(
        getConfigFactory().build(ClientConfig.class), getPhoneBook(), view
    );
    lifecycle.addManagedInstance(clientInventoryManager);

    final CachingClusteredClient baseClient = new CachingClusteredClient(warehouse, view, cacheBroker, getSmileMapper());
    lifecycle.addManagedInstance(baseClient);


    final ClientQuerySegmentWalker texasRanger = new ClientQuerySegmentWalker(warehouse, getEmitter(), baseClient);

    List<Module> theModules = Lists.newArrayList();
    theModules.add(new ClientServletModule(texasRanger, clientInventoryManager, getJsonMapper()));
    theModules.addAll(extraModules);

    final Injector injector = Guice.createInjector(theModules);
    final Context root = new Context(getServer(), "/druid/v2", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(
        new ServletHolder(new QueryServlet(getJsonMapper(), getSmileMapper(), texasRanger, getEmitter(), getRequestLogger())),
        "/*"
    );

    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GuiceFilter.class, "/datasources/*", 0);

    for (String path : pathsForGuiceFilter) {
      root.addFilter(GuiceFilter.class, path, 0);
    }
  }

  private void initializeDiscovery() throws Exception
  {
    if (useDiscovery) {
      final Lifecycle lifecycle = getLifecycle();

      final ServiceDiscoveryConfig serviceDiscoveryConfig = getConfigFactory().build(ServiceDiscoveryConfig.class);
      CuratorFramework curatorFramework = Initialization.makeCuratorFrameworkClient(
          serviceDiscoveryConfig, lifecycle
      );

      final ServiceDiscovery serviceDiscovery = Initialization.makeServiceDiscoveryClient(
          curatorFramework, serviceDiscoveryConfig, lifecycle
      );
    }
  }

  private void initializeCacheBroker()
  {
    if (cacheBroker == null) {
      setCacheBroker(
          MapCacheBroker.create(
              getConfigFactory().buildWithReplacements(
                  MapCacheBrokerConfig.class,
                  ImmutableMap.of("prefix", "druid.bard.cache")
              )
          )
      );
    }
  }

  private void initializeBrokerHttpClient()
  {
    if (brokerHttpClient == null) {
      setBrokerHttpClient(
          HttpClientInit.createClient(
              HttpClientConfig
                  .builder()
                  .withNumConnections(PropUtils.getPropertyAsInt(getProps(), "druid.client.http.connections"))
                  .build(),
              getLifecycle()
          )
      );
    }
  }

  private void initializeWarehouse()
  {
    if (warehouse == null) {
      setWarehouse(new ReflectionQueryToolChestWarehouse());
    }
  }

  public static class Builder
  {
    private ObjectMapper jsonMapper = null;
    private ObjectMapper smileMapper = null;
    private Lifecycle lifecycle = null;
    private Properties props = null;
    private ConfigurationObjectFactory configFactory = null;

    public Builder withMappers(ObjectMapper jsonMapper, ObjectMapper smileMapper)
    {
      this.jsonMapper = jsonMapper;
      this.smileMapper = smileMapper;
      return this;
    }

    public Builder withProps(Properties props)
    {
      this.props = props;
      return this;
    }

    public Builder withConfigFactory(ConfigurationObjectFactory configFactory)
    {
      this.configFactory = configFactory;
      return this;
    }

    public BrokerNode build()
    {
      if (jsonMapper == null && smileMapper == null) {
        jsonMapper = new DefaultObjectMapper();
        smileMapper = new DefaultObjectMapper(new SmileFactory());
        smileMapper.getJsonFactory().setCodec(smileMapper);
      }
      else if (jsonMapper == null || smileMapper == null) {
        throw new ISE("Only jsonMapper[%s] or smileMapper[%s] was set, must set neither or both.", jsonMapper, smileMapper);
      }

      if (lifecycle == null) {
        lifecycle = new Lifecycle();
      }

      if (props == null) {
        props = Initialization.loadProperties();
      }

      if (configFactory == null) {
        configFactory = Config.createFactory(props);
      }

      return new BrokerNode(props, lifecycle, jsonMapper, smileMapper, configFactory);
    }
  }
}
