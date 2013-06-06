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

package com.metamx.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.QueryableNode;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.http.GuiceServletConfig;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.indexing.common.config.IndexerZkConfig;
import com.metamx.druid.indexing.common.config.TaskLogConfig;
import com.metamx.druid.indexing.common.index.EventReceiverFirehoseFactory;
import com.metamx.druid.indexing.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.indexing.common.tasklogs.NoopTaskLogs;
import com.metamx.druid.indexing.common.tasklogs.S3TaskLogs;
import com.metamx.druid.indexing.common.tasklogs.TaskLogs;
import com.metamx.druid.indexing.coordinator.ForkingTaskRunner;
import com.metamx.druid.indexing.coordinator.config.ForkingTaskRunnerConfig;
import com.metamx.druid.indexing.worker.Worker;
import com.metamx.druid.indexing.worker.WorkerCuratorCoordinator;
import com.metamx.druid.indexing.worker.WorkerTaskMonitor;
import com.metamx.druid.indexing.worker.config.WorkerConfig;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceProvider;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.Duration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerNode extends QueryableNode<WorkerNode>
{
  private static final EmittingLogger log = new EmittingLogger(WorkerNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private RestS3Service s3Service = null;
  private List<Monitor> monitors = null;
  private HttpClient httpClient = null;
  private ServiceEmitter emitter = null;
  private WorkerConfig workerConfig = null;
  private ServiceDiscovery serviceDiscovery = null;
  private ServiceAnnouncer serviceAnnouncer = null;
  private ServiceProvider coordinatorServiceProvider = null;
  private WorkerCuratorCoordinator workerCuratorCoordinator = null;
  private WorkerTaskMonitor workerTaskMonitor = null;
  private TaskLogs persistentTaskLogs = null;
  private ForkingTaskRunner forkingTaskRunner = null;
  private Server server = null;

  private boolean initialized = false;

  public WorkerNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super("indexer-worker", log, props, lifecycle, jsonMapper, smileMapper, configFactory);
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

  public WorkerNode setS3Service(RestS3Service s3Service)
  {
    this.s3Service = s3Service;
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
    initializeServiceDiscovery();
    initializeJacksonInjections();
    initializeJacksonSubtypes();
    initializeCuratorCoordinator();
    initializePersistentTaskLogs();
    initializeTaskRunner();
    initializeWorkerTaskMonitor();
    initializeServer();

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(getLifecycle());
    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        getConfigFactory().build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    getLifecycle().addManagedInstance(monitorScheduler);

    final Injector injector = Guice.createInjector(
        new WorkerServletModule(
            getJsonMapper(),
            emitter,
            forkingTaskRunner
        )
    );
    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GuiceFilter.class, "/druid/worker/v1/*", 0);
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (!initialized) {
      doInit();
    }

    getLifecycle().start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    getLifecycle().stop();
  }

  private void initializeServer()
  {
    if (server == null) {
      server = Initialization.makeJettyServer(getConfigFactory().build(ServerConfig.class));

      getLifecycle().addHandler(
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

  private void initializeJacksonInjections()
  {
    InjectableValues.Std injectables = new InjectableValues.Std();

    injectables.addValue("s3Client", null)
               .addValue("segmentPusher", null)
               .addValue("chatHandlerProvider", null);

    getJsonMapper().setInjectableValues(injectables);
  }

  private void initializeJacksonSubtypes()
  {
    getJsonMapper().registerSubtypes(StaticS3FirehoseFactory.class);
    getJsonMapper().registerSubtypes(EventReceiverFirehoseFactory.class);
  }

  private void initializeHttpClient()
  {
    if (httpClient == null) {
      httpClient = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1)
                          .withReadTimeout(new Duration(PropUtils.getProperty(getProps(), "druid.emitter.timeOut")))
                          .build(), getLifecycle()
      );
    }
  }

  private void initializeEmitter()
  {
    if (emitter == null) {
      emitter = new ServiceEmitter(
          PropUtils.getProperty(getProps(), "druid.service"),
          PropUtils.getProperty(getProps(), "druid.host"),
          Emitters.create(getProps(), httpClient, getJsonMapper(), getLifecycle())
      );
    }
    EmittingLogger.registerEmitter(emitter);
  }

  private void initializeS3Service() throws S3ServiceException
  {
    if (s3Service == null) {
      s3Service = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(getProps(), "com.metamx.aws.accessKey"),
              PropUtils.getProperty(getProps(), "com.metamx.aws.secretKey")
          )
      );
    }
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
    if (workerConfig == null) {
      workerConfig = getConfigFactory().build(WorkerConfig.class);
    }
  }

  public void initializeServiceDiscovery() throws Exception
  {
    if (serviceDiscovery == null) {
      final ServiceDiscoveryConfig config = getConfigFactory().build(ServiceDiscoveryConfig.class);
      this.serviceDiscovery = Initialization.makeServiceDiscoveryClient(
          getCuratorFramework(),
          config,
          getLifecycle()
      );
    }
    if (coordinatorServiceProvider == null) {
      this.coordinatorServiceProvider = Initialization.makeServiceProvider(
          workerConfig.getMasterService(),
          serviceDiscovery,
          getLifecycle()
      );
    }
  }

  public void initializeCuratorCoordinator()
  {
    if (workerCuratorCoordinator == null) {
      workerCuratorCoordinator = new WorkerCuratorCoordinator(
          getJsonMapper(),
          getConfigFactory().build(IndexerZkConfig.class),
          getCuratorFramework(),
          new Worker(workerConfig)
      );
      getLifecycle().addManagedInstance(workerCuratorCoordinator);
    }
  }

  private void initializePersistentTaskLogs() throws S3ServiceException
  {
    if (persistentTaskLogs == null) {
      final TaskLogConfig taskLogConfig = getConfigFactory().build(TaskLogConfig.class);
      if (taskLogConfig.getLogStorageBucket() != null) {
        initializeS3Service();
        persistentTaskLogs = new S3TaskLogs(
            taskLogConfig.getLogStorageBucket(),
            taskLogConfig.getLogStoragePrefix(),
            s3Service
        );
      } else {
        persistentTaskLogs = new NoopTaskLogs();
      }
    }
  }

  public void initializeTaskRunner()
  {
    if (forkingTaskRunner == null) {
      forkingTaskRunner = new ForkingTaskRunner(
          getConfigFactory().build(ForkingTaskRunnerConfig.class),
          getProps(),
          persistentTaskLogs,
          Executors.newFixedThreadPool(workerConfig.getCapacity()),
          getJsonMapper()
      );
    }
  }

  public void initializeWorkerTaskMonitor()
  {
    if (workerTaskMonitor == null) {
      final ExecutorService workerExec = Executors.newFixedThreadPool(workerConfig.getNumThreads());
      final CuratorFramework curatorFramework = getCuratorFramework();

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
      getLifecycle().addManagedInstance(workerTaskMonitor);
    }
  }

  public static class Builder
  {
    private ObjectMapper jsonMapper = null;
    private ObjectMapper smileMapper = null;
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
      if (jsonMapper == null && smileMapper == null) {
        jsonMapper = new DefaultObjectMapper();
        smileMapper = new DefaultObjectMapper(new SmileFactory());
        smileMapper.getJsonFactory().setCodec(smileMapper);
      } else if (jsonMapper == null || smileMapper == null) {
        throw new ISE(
            "Only jsonMapper[%s] or smileMapper[%s] was set, must set neither or both.",
            jsonMapper,
            smileMapper
        );
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

      return new WorkerNode(props, lifecycle, jsonMapper, smileMapper, configFactory);
    }
  }
}
