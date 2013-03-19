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

package com.metamx.druid.merger.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.RegisteringNode;
import com.metamx.druid.http.GuiceServletConfig;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.CuratorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.config.TaskConfig;
import com.metamx.druid.merger.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.merger.coordinator.ForkingTaskRunner;
import com.metamx.druid.merger.coordinator.config.ForkingTaskRunnerConfig;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.druid.merger.worker.WorkerCuratorCoordinator;
import com.metamx.druid.merger.worker.WorkerTaskMonitor;
import com.metamx.druid.merger.worker.config.WorkerConfig;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.EmittingLogger;
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
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerNode extends RegisteringNode
{
  private static final EmittingLogger log = new EmittingLogger(WorkerNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Lifecycle lifecycle;
  private final Properties props;
  private final ObjectMapper jsonMapper;
  private final ConfigurationObjectFactory configFactory;

  private List<Monitor> monitors = null;
  private HttpClient httpClient = null;
  private ServiceEmitter emitter = null;
  private TaskConfig taskConfig = null;
  private WorkerConfig workerConfig = null;
  private CuratorFramework curatorFramework = null;
  private ServiceDiscovery serviceDiscovery = null;
  private ServiceProvider coordinatorServiceProvider = null;
  private WorkerCuratorCoordinator workerCuratorCoordinator = null;
  private WorkerTaskMonitor workerTaskMonitor = null;
  private ForkingTaskRunner forkingTaskRunner = null;
  private Server server = null;

  private boolean initialized = false;

  public WorkerNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(ImmutableList.of(jsonMapper));

    this.lifecycle = lifecycle;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.configFactory = configFactory;
  }

  public WorkerNode setHttpClient(HttpClient httpClient)
  {
    this.httpClient = httpClient;
    return this;
  }

  public WorkerNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public WorkerNode setCuratorFramework(CuratorFramework curatorFramework)
  {
    this.curatorFramework = curatorFramework;
    return this;
  }

  public WorkerNode setCoordinatorServiceProvider(ServiceProvider coordinatorServiceProvider)
  {
    this.coordinatorServiceProvider = coordinatorServiceProvider;
    return this;
  }

  public WorkerNode setServiceDiscovery(ServiceDiscovery serviceDiscovery)
  {
    this.serviceDiscovery = serviceDiscovery;
    return this;
  }

  public WorkerNode setWorkerCuratorCoordinator(WorkerCuratorCoordinator workerCuratorCoordinator)
  {
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    return this;
  }

  public WorkerNode setForkingTaskRunner(ForkingTaskRunner forkingTaskRunner)
  {
    this.forkingTaskRunner = forkingTaskRunner;
    return this;
  }

  public WorkerNode setWorkerTaskMonitor(WorkerTaskMonitor workerTaskMonitor)
  {
    this.workerTaskMonitor = workerTaskMonitor;
    return this;
  }

  public void doInit() throws Exception
  {
    initializeHttpClient();
    initializeEmitter();
    initializeMonitors();
    initializeMergerConfig();
    initializeCuratorFramework();
    initializeServiceDiscovery();
    initializeCoordinatorServiceProvider();
    initializeJacksonSubtypes();
    initializeCuratorCoordinator();
    initializeTaskRunner();
    initializeWorkerTaskMonitor();
    initializeServer();

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(monitorScheduler);

    final Injector injector = Guice.createInjector(
        new WorkerServletModule(
            getJsonMapper(),
            emitter,
            forkingTaskRunner
        )
    );
    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GuiceFilter.class, "/mmx/worker/v1/*", 0);
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (!initialized) {
      doInit();
    }

    lifecycle.start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    lifecycle.stop();
  }

  private ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  private void initializeServer()
  {
    if (server == null) {
      server = Initialization.makeJettyServer(configFactory.build(ServerConfig.class));

      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              log.info("Starting Jetty");
              server.start();
            }

            @Override
            public void stop()
            {
              log.info("Stopping Jetty");
              try {
                server.stop();
              }
              catch (Exception e) {
                log.error(e, "Exception thrown while stopping Jetty");
              }
            }
          }
      );
    }
  }

  private void initializeJacksonSubtypes()
  {
    getJsonMapper().registerSubtypes(StaticS3FirehoseFactory.class);
  }

  private void initializeHttpClient()
  {
    if (httpClient == null) {
      httpClient = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
      );
    }
  }

  private void initializeEmitter()
  {
    if (emitter == null) {
      emitter = new ServiceEmitter(
          PropUtils.getProperty(props, "druid.service"),
          PropUtils.getProperty(props, "druid.host"),
          Emitters.create(props, httpClient, getJsonMapper(), lifecycle)
      );
    }
    EmittingLogger.registerEmitter(emitter);
  }

  private void initializeMonitors()
  {
    if (monitors == null) {
      monitors = Lists.newArrayList();
      monitors.add(new JvmMonitor());
      monitors.add(new SysMonitor());
    }
  }

  private void initializeMergerConfig()
  {
    if (taskConfig == null) {
      taskConfig = configFactory.build(TaskConfig.class);
    }

    if (workerConfig == null) {
      workerConfig = configFactory.build(WorkerConfig.class);
    }
  }

  public void initializeCuratorFramework() throws IOException
  {
    if (curatorFramework == null) {
      final CuratorConfig curatorConfig = configFactory.build(CuratorConfig.class);
      curatorFramework = Initialization.makeCuratorFrameworkClient(
          curatorConfig,
          lifecycle
      );
    }
  }

  public void initializeServiceDiscovery() throws Exception
  {
    if (serviceDiscovery == null) {
      final ServiceDiscoveryConfig config = configFactory.build(ServiceDiscoveryConfig.class);
      this.serviceDiscovery = Initialization.makeServiceDiscoveryClient(
          curatorFramework,
          config,
          lifecycle
      );
    }
  }

  public void initializeCoordinatorServiceProvider()
  {
    if (coordinatorServiceProvider == null) {
      this.coordinatorServiceProvider = Initialization.makeServiceProvider(
          workerConfig.getMasterService(),
          serviceDiscovery,
          lifecycle
      );
    }
  }

  public void initializeCuratorCoordinator()
  {
    if (workerCuratorCoordinator == null) {
      workerCuratorCoordinator = new WorkerCuratorCoordinator(
          getJsonMapper(),
          configFactory.build(IndexerZkConfig.class),
          curatorFramework,
          new Worker(workerConfig)
      );
      lifecycle.addManagedInstance(workerCuratorCoordinator);
    }
  }

  public void initializeTaskRunner()
  {
    if (forkingTaskRunner == null) {
      forkingTaskRunner = new ForkingTaskRunner(
          configFactory.build(ForkingTaskRunnerConfig.class),
          Executors.newFixedThreadPool(workerConfig.getCapacity()),
          getJsonMapper()
      );
    }
  }

  public void initializeWorkerTaskMonitor()
  {
    if (workerTaskMonitor == null) {
      final ExecutorService workerExec = Executors.newFixedThreadPool(workerConfig.getNumThreads());
      final PathChildrenCache pathChildrenCache = new PathChildrenCache(
          curatorFramework,
          workerCuratorCoordinator.getTaskPathForWorker(),
          false
      );
      workerTaskMonitor = new WorkerTaskMonitor(
          getJsonMapper(),
          pathChildrenCache,
          curatorFramework,
          workerCuratorCoordinator,
          forkingTaskRunner,
          workerExec
      );
      lifecycle.addManagedInstance(workerTaskMonitor);
    }
  }

  public static class Builder
  {
    private ObjectMapper jsonMapper = null;
    private Lifecycle lifecycle = null;
    private Properties props = null;
    private ConfigurationObjectFactory configFactory = null;

    public Builder withMapper(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public Builder withLifecycle(Lifecycle lifecycle)
    {
      this.lifecycle = lifecycle;
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

    public WorkerNode build()
    {
      if (jsonMapper == null) {
        jsonMapper = new DefaultObjectMapper();
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

      return new WorkerNode(props, lifecycle, jsonMapper, configFactory);
    }
  }
}
