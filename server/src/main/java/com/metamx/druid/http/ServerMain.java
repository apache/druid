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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.concurrent.ExecutorServices;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.Query;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.druid.coordination.ZkCoordinator;
import com.metamx.druid.coordination.ZkCoordinatorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.initialization.ZkClientConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.QueryableLoaderConfig;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.metrics.ServerMonitor;
import com.metamx.druid.query.DefaultQueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.emitter.core.Emitters;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import com.metamx.metrics.SysMonitor;
import com.metamx.phonebook.PhoneBook;

/**
 */
public class ServerMain
{
  private static final Logger log = new Logger(ServerMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final ObjectMapper smileMapper = new DefaultObjectMapper(new SmileFactory());
    smileMapper.getJsonFactory().setCodec(smileMapper);

    final Properties props = Initialization.loadProperties();
    final ConfigurationObjectFactory configFactory = Config.createFactory(props);
    final Lifecycle lifecycle = new Lifecycle();

    final HttpClient httpClient = HttpClientInit.createClient(
        HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
    );

    final ServiceEmitter emitter = new ServiceEmitter(
        props.getProperty("druid.service"),
        props.getProperty("druid.host"),
        Emitters.create(props, httpClient, jsonMapper, lifecycle)
    );

    final ExecutorService executorService = ExecutorServices.create(
        lifecycle,
        configFactory.buildWithReplacements(
            ExecutorServiceConfig.class, ImmutableMap.of("base_path", "druid.processing")
        )
    );

    StupidPool<ByteBuffer> computationBufferPool = ServerInit.makeComputeScratchPool(
        Integer.parseInt(props.getProperty("druid.computation.buffer.size", String.valueOf(1024 * 1024 * 1024)))
    );

    Map<Class<? extends Query>, QueryRunnerFactory> queryRunners = ServerInit.initDefaultQueryTypes(
        configFactory,
        computationBufferPool
    );

    final RestS3Service s3Client = new RestS3Service(
        new AWSCredentials(props.getProperty("com.metamx.aws.accessKey"), props.getProperty("com.metamx.aws.secretKey"))
    );
    QueryableLoaderConfig queryableLoaderConfig = configFactory.build(QueryableLoaderConfig.class);
    final ServerManager serverManager = new ServerManager(
        ServerInit.makeDefaultQueryableLoader(s3Client, queryableLoaderConfig),
        new DefaultQueryRunnerFactoryConglomerate(queryRunners),
        emitter,
        executorService
    );

    final ZkClient zkClient = Initialization.makeZkClient(configFactory.build(ZkClientConfig.class), lifecycle);

    final DruidServer druidServer = new DruidServer(configFactory.build(DruidServerConfig.class));
    final PhoneBook coordinatorYp = Initialization.createYellowPages(
        jsonMapper,
        zkClient,
        "Coordinator-ZKYP--%s",
        lifecycle
    );
    final ZkCoordinator coordinator = new ZkCoordinator(
        jsonMapper,
        configFactory.build(ZkCoordinatorConfig.class),
        druidServer,
        coordinatorYp,
        serverManager,
        emitter
    );
    lifecycle.addManagedInstance(coordinator);

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);

    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final List<Monitor> monitors = Lists.<Monitor>newArrayList(
        new ServerMonitor(druidServer, serverManager),
        new JvmMonitor()
    );
    if (Boolean.parseBoolean(props.getProperty("druid.monitoring.monitorSystem", "true"))) {
      monitors.add(new SysMonitor());
    }

    final MonitorScheduler healthMonitor = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(healthMonitor);

    final RequestLogger requestLogger = Initialization.makeRequestLogger(
        scheduledExecutorFactory.create(
            1,
            "RequestLogger--%d"
        ),
        props
    );
    lifecycle.addManagedInstance(requestLogger);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.error(t, "Error when starting up.  Failing.");
      System.exit(1);
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Running shutdown hook");
                lifecycle.stop();
              }
            }
        )
    );

    final Server server = Initialization.makeJettyServer(configFactory.build(ServerConfig.class));
    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(
        new ServletHolder(new QueryServlet(jsonMapper, smileMapper, serverManager, emitter, requestLogger)),
        "/*"
    );


    server.start();
    server.join();
  }
}
