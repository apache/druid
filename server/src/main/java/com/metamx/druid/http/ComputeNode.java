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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.concurrent.ExecutorServices;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.BaseServerNode;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.druid.coordination.ZkCoordinator;
import com.metamx.druid.coordination.ZkCoordinatorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.QueryableLoaderConfig;
import com.metamx.druid.loading.StorageAdapterLoader;
import com.metamx.druid.metrics.ServerMonitor;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.Monitor;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 */
public class ComputeNode extends BaseServerNode<ComputeNode>
{
  private static final Logger log = new Logger(ComputeNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private DruidServer druidServer;
  private StorageAdapterLoader adapterLoader;

  public ComputeNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }

  public ComputeNode setAdapterLoader(StorageAdapterLoader storageAdapterLoader)
  {
    Preconditions.checkState(this.adapterLoader == null, "Cannot set adapterLoader once it has already been set.");
    this.adapterLoader = storageAdapterLoader;
    return this;
  }

  public ComputeNode setDruidServer(DruidServer druidServer)
  {
    Preconditions.checkState(this.druidServer == null, "Cannot set druidServer once it has already been set.");
    this.druidServer = druidServer;
    return this;
  }

  public DruidServer getDruidServer()
  {
    initializeDruidServer();
    return druidServer;
  }

  public StorageAdapterLoader getAdapterLoader()
  {
    initializeAdapterLoader();
    return adapterLoader;
  }

  protected void doInit() throws Exception
  {
    initializeDruidServer();
    initializeAdapterLoader();

    final Lifecycle lifecycle = getLifecycle();
    final ServiceEmitter emitter = getEmitter();
    final List<Monitor> monitors = getMonitors();
    final QueryRunnerFactoryConglomerate conglomerate = getConglomerate();

    final ExecutorService executorService = ExecutorServices.create(
        getLifecycle(),
        getConfigFactory().buildWithReplacements(
            ExecutorServiceConfig.class, ImmutableMap.of("base_path", "druid.processing")
        )
    );
    ServerManager serverManager = new ServerManager(adapterLoader, conglomerate, emitter, executorService);

    final ZkCoordinator coordinator = new ZkCoordinator(
        getJsonMapper(),
        getConfigFactory().build(ZkCoordinatorConfig.class),
        druidServer,
        getPhoneBook(),
        serverManager,
        emitter
    );
    lifecycle.addManagedInstance(coordinator);

    monitors.add(new ServerMonitor(getDruidServer(), serverManager));
    startMonitoring(monitors);

    final Context root = new Context(getServer(), "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(
        new ServletHolder(
            new QueryServlet(getJsonMapper(), getSmileMapper(), serverManager, emitter, getRequestLogger())
        ),
        "/*"
    );
  }

  private void initializeAdapterLoader()
  {
    if (adapterLoader == null) {
      final Properties props = getProps();
      try {
        final RestS3Service s3Client = new RestS3Service(
            new AWSCredentials(
                PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
                PropUtils.getProperty(props, "com.metamx.aws.secretKey")
            )
        );

        setAdapterLoader(
            ServerInit.makeDefaultQueryableLoader(s3Client, getConfigFactory().build(QueryableLoaderConfig.class))
        );
      }
      catch (S3ServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void initializeDruidServer()
  {
    if (druidServer == null) {
      setDruidServer(new DruidServer(getConfigFactory().build(DruidServerConfig.class), "historical"));
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

    public ComputeNode build()
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

      return new ComputeNode(props, lifecycle, jsonMapper, smileMapper, configFactory);
    }
  }

}
