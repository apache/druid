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

package com.metamx.druid.merger.coordinator.http;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import com.metamx.common.logger.Logger;
import com.metamx.druid.RegisteringNode;
import com.metamx.druid.config.ConfigManager;
import com.metamx.druid.config.ConfigManagerConfig;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.http.GuiceServletConfig;
import com.metamx.druid.http.RedirectFilter;
import com.metamx.druid.http.RedirectInfo;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.RetryPolicyFactory;
import com.metamx.druid.merger.common.actions.LocalTaskActionClientFactory;
import com.metamx.druid.merger.common.actions.TaskActionClientFactory;
import com.metamx.druid.merger.common.actions.TaskActionToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.config.RetryPolicyConfig;
import com.metamx.druid.merger.common.config.TaskLogConfig;
import com.metamx.druid.merger.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.merger.common.tasklogs.NoopTaskLogs;
import com.metamx.druid.merger.common.tasklogs.S3TaskLogs;
import com.metamx.druid.merger.common.tasklogs.SwitchingTaskLogProvider;
import com.metamx.druid.merger.common.tasklogs.TaskLogProvider;
import com.metamx.druid.merger.common.tasklogs.TaskLogs;
import com.metamx.druid.merger.coordinator.DbTaskStorage;
import com.metamx.druid.merger.coordinator.ForkingTaskRunner;
import com.metamx.druid.merger.coordinator.HeapMemoryTaskStorage;
import com.metamx.druid.merger.coordinator.MergerDBCoordinator;
import com.metamx.druid.merger.coordinator.RemoteTaskRunner;
import com.metamx.druid.merger.coordinator.TaskLockbox;
import com.metamx.druid.merger.coordinator.TaskMasterLifecycle;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.TaskRunner;
import com.metamx.druid.merger.coordinator.TaskRunnerFactory;
import com.metamx.druid.merger.coordinator.TaskStorage;
import com.metamx.druid.merger.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.merger.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.druid.merger.coordinator.config.ForkingTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.config.IndexerDbConnectorConfig;
import com.metamx.druid.merger.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.scaling.AutoScalingStrategy;
import com.metamx.druid.merger.coordinator.scaling.EC2AutoScalingStrategy;
import com.metamx.druid.merger.coordinator.scaling.NoopAutoScalingStrategy;
import com.metamx.druid.merger.coordinator.scaling.ResourceManagementScheduler;
import com.metamx.druid.merger.coordinator.scaling.ResourceManagementSchedulerConfig;
import com.metamx.druid.merger.coordinator.scaling.ResourceManagementSchedulerFactory;
import com.metamx.druid.merger.coordinator.scaling.SimpleResourceManagementStrategy;
import com.metamx.druid.merger.coordinator.scaling.SimpleResourceManagmentConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitters;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import com.metamx.metrics.SysMonitor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.Duration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.resource.ResourceCollection;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class IndexerCoordinatorNode extends RegisteringNode
{
  private static final Logger log = new Logger(IndexerCoordinatorNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Lifecycle lifecycle;
  private final ObjectMapper jsonMapper;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private RestS3Service s3Service = null;
  private List<Monitor> monitors = null;
  private ServiceEmitter emitter = null;
  private DbConnectorConfig dbConnectorConfig = null;
  private DBI dbi = null;
  private IndexerCoordinatorConfig config = null;
  private MergerDBCoordinator mergerDBCoordinator = null;
  private TaskStorage taskStorage = null;
  private TaskQueue taskQueue = null;
  private TaskLockbox taskLockbox = null;
  private CuratorFramework curatorFramework = null;
  private IndexerZkConfig indexerZkConfig;
  private TaskRunnerFactory taskRunnerFactory = null;
  private ResourceManagementSchedulerFactory resourceManagementSchedulerFactory = null;
  private HttpClient httpClient = null;
  private TaskActionClientFactory taskActionClientFactory = null;
  private TaskMasterLifecycle taskMasterLifecycle = null;
  private TaskLogs persistentTaskLogs = null;
  private TaskLogProvider taskLogProvider = null;
  private Server server = null;

  private boolean initialized = false;

  public IndexerCoordinatorNode(
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

  public IndexerCoordinatorNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public IndexerCoordinatorNode setMergerDBCoordinator(MergerDBCoordinator mergerDBCoordinator)
  {
    this.mergerDBCoordinator = mergerDBCoordinator;
    return this;
  }

  public IndexerCoordinatorNode setS3Service(RestS3Service s3Service)
  {
    this.s3Service = s3Service;
    return this;
  }

  public IndexerCoordinatorNode setTaskQueue(TaskQueue taskQueue)
  {
    this.taskQueue = taskQueue;
    return this;
  }

  public IndexerCoordinatorNode setTaskLockbox(TaskLockbox taskLockbox)
  {
    this.taskLockbox = taskLockbox;
    return this;
  }

  public IndexerCoordinatorNode setMergeDbCoordinator(MergerDBCoordinator mergeDbCoordinator)
  {
    this.mergerDBCoordinator = mergeDbCoordinator;
    return this;
  }

  public IndexerCoordinatorNode setCuratorFramework(CuratorFramework curatorFramework)
  {
    this.curatorFramework = curatorFramework;
    return this;
  }

  public IndexerCoordinatorNode setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory)
  {
    this.taskRunnerFactory = taskRunnerFactory;
    return this;
  }

  public IndexerCoordinatorNode setResourceManagementSchedulerFactory(ResourceManagementSchedulerFactory resourceManagementSchedulerFactory)
  {
    this.resourceManagementSchedulerFactory = resourceManagementSchedulerFactory;
    return this;
  }

  public IndexerCoordinatorNode setHttpClient(HttpClient httpClient)
  {
    this.httpClient = httpClient;
    return this;
  }

  public void doInit() throws Exception
  {
    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
    initializeDB();

    final ConfigManagerConfig managerConfig = configFactory.build(ConfigManagerConfig.class);
    DbConnector.createConfigTable(dbi, managerConfig.getConfigTable());
    JacksonConfigManager configManager =
        new JacksonConfigManager(
            lifecycle.addManagedInstance(
                new ConfigManager(
                    dbi,
                    managerConfig
                )
            ), getJsonMapper()
        );

    initializeHttpClient();
    initializeEmitter();
    initializeMonitors();
    initializeIndexerCoordinatorConfig();
    initializeMergeDBCoordinator();
    initializeTaskStorage();
    initializeTaskLockbox();
    initializeTaskQueue();
    initializeJacksonSubtypes();
    initializeCurator();
    initializeIndexerZkConfig();
    initializeTaskActionClientFactory();
    initializeTaskRunnerFactory(configManager);
    initializeResourceManagement(configManager);
    initializeTaskMasterLifecycle();
    initializePersistentTaskLogs();
    initializeTaskLogProvider();
    initializeServer();

    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(monitorScheduler);

    final Injector injector = Guice.createInjector(
        new IndexerCoordinatorServletModule(
            getJsonMapper(),
            config,
            emitter,
            taskMasterLifecycle,
            new TaskStorageQueryAdapter(taskStorage),
            taskLogProvider,
            configManager
        )
    );

    final Context staticContext = new Context(server, "/static", Context.SESSIONS);
    staticContext.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    ResourceCollection resourceCollection = new ResourceCollection(
        new String[]{
            IndexerCoordinatorNode.class.getClassLoader().getResource("static").toExternalForm(),
            IndexerCoordinatorNode.class.getClassLoader().getResource("indexer_static").toExternalForm()
        }
    );
    staticContext.setBaseResource(resourceCollection);

    // TODO -- Need a QueryServlet and some kind of QuerySegmentWalker if we want to support querying tasks
    // TODO -- (e.g. for realtime) in local mode

    final Context root = new Context(server, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/mmx/*");
    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(
        new FilterHolder(
            new RedirectFilter(
                new ToStringResponseHandler(Charsets.UTF_8),
                new RedirectInfo()
                {
                  @Override
                  public boolean doLocal()
                  {
                    return taskMasterLifecycle.isLeading();
                  }

                  @Override
                  public URL getRedirectURL(String queryString, String requestURI)
                  {
                    try {
                      return new URL(
                          String.format(
                              "http://%s%s",
                              taskMasterLifecycle.getLeader(),
                              requestURI
                          )
                      );
                    }
                    catch (Exception e) {
                      throw Throwables.propagate(e);
                    }
                  }
                }
            )
        ), "/*", 0
    );
    root.addFilter(GuiceFilter.class, "/mmx/merger/v1/*", 0);

    initialized = true;
  }

  private ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  private void initializeTaskActionClientFactory()
  {
    if (taskActionClientFactory == null) {
      taskActionClientFactory = new LocalTaskActionClientFactory(
          taskStorage,
          new TaskActionToolbox(taskQueue, taskLockbox, mergerDBCoordinator, emitter)
      );
    }
  }

  private void initializeTaskMasterLifecycle()
  {
    if (taskMasterLifecycle == null) {
      final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
      taskMasterLifecycle = new TaskMasterLifecycle(
          taskQueue,
          taskActionClientFactory,
          config,
          serviceDiscoveryConfig,
          taskRunnerFactory,
          resourceManagementSchedulerFactory,
          curatorFramework,
          emitter
      );
      lifecycle.addManagedInstance(taskMasterLifecycle);
    }
  }

  private void initializePersistentTaskLogs() throws S3ServiceException
  {
    if (persistentTaskLogs == null) {
      final TaskLogConfig taskLogConfig = configFactory.build(TaskLogConfig.class);
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

  private void initializeTaskLogProvider()
  {
    if (taskLogProvider == null) {
      final List<TaskLogProvider> providers = Lists.newArrayList();

      // Use our TaskRunner if it is also a TaskLogProvider
      providers.add(
          new TaskLogProvider()
          {
            @Override
            public Optional<InputSupplier<InputStream>> streamTaskLog(String taskid, long offset) throws IOException
            {
              final TaskRunner runner = taskMasterLifecycle.getTaskRunner().orNull();
              if (runner instanceof TaskLogProvider) {
                return ((TaskLogProvider) runner).streamTaskLog(taskid, offset);
              } else {
                return Optional.absent();
              }
            }
          }
      );

      // Use our persistent log storage
      providers.add(persistentTaskLogs);

      taskLogProvider = new SwitchingTaskLogProvider(providers);
    }
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
          HttpClientConfig.builder().withNumConnections(1).withReadTimeout(
              new Duration(
                  PropUtils.getProperty(
                      props,
                      "druid.emitter.timeOut"
                  )
              )
          ).build(), lifecycle
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

  private void initializeS3Service() throws S3ServiceException
  {
    if(s3Service == null) {
      s3Service = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
              PropUtils.getProperty(props, "com.metamx.aws.secretKey")
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

  private void initializeDB()
  {
    if (dbConnectorConfig == null) {
      dbConnectorConfig = configFactory.build(DbConnectorConfig.class);
    }
    if (dbi == null) {
      dbi = new DbConnector(dbConnectorConfig).getDBI();
    }
  }

  private void initializeIndexerCoordinatorConfig()
  {
    if (config == null) {
      config = configFactory.build(IndexerCoordinatorConfig.class);
    }
  }

  public void initializeMergeDBCoordinator()
  {
    if (mergerDBCoordinator == null) {
      mergerDBCoordinator = new MergerDBCoordinator(
          getJsonMapper(),
          dbConnectorConfig,
          dbi
      );
    }
  }

  public void initializeTaskQueue()
  {
    if (taskQueue == null) {
      // Don't start it here. The TaskMasterLifecycle will handle that when it feels like it.
      taskQueue = new TaskQueue(taskStorage, taskLockbox);
    }
  }

  public void initializeTaskLockbox()
  {
    if (taskLockbox == null) {
      taskLockbox = new TaskLockbox(taskStorage);
    }
  }

  public void initializeCurator() throws Exception
  {
    if (curatorFramework == null) {
      final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
      curatorFramework = Initialization.makeCuratorFrameworkClient(
          serviceDiscoveryConfig,
          lifecycle
      );
    }
  }

  public void initializeIndexerZkConfig()
  {
    if (indexerZkConfig == null) {
      indexerZkConfig = configFactory.build(IndexerZkConfig.class);
    }
  }

  public void initializeTaskStorage()
  {
    if (taskStorage == null) {
      if (config.getStorageImpl().equals("local")) {
        taskStorage = new HeapMemoryTaskStorage();
      } else if (config.getStorageImpl().equals("db")) {
        final IndexerDbConnectorConfig dbConnectorConfig = configFactory.build(IndexerDbConnectorConfig.class);
        taskStorage = new DbTaskStorage(
            getJsonMapper(),
            dbConnectorConfig,
            new DbConnector(dbConnectorConfig).getDBI()
        );
      } else {
        throw new ISE("Invalid storage implementation: %s", config.getStorageImpl());
      }
    }
  }

  private void initializeTaskRunnerFactory(final JacksonConfigManager configManager)
  {
    if (taskRunnerFactory == null) {
      if (config.getRunnerImpl().equals("remote")) {
        taskRunnerFactory = new TaskRunnerFactory()
        {
          @Override
          public TaskRunner build()
          {
            // Don't use scheduledExecutorFactory, since it's linked to the wrong lifecycle (global lifecycle instead
            // of leadership lifecycle)
            final ScheduledExecutorService retryScheduledExec = Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("RemoteRunnerRetryExec--%d")
                    .build()
            );

            return new RemoteTaskRunner(
                getJsonMapper(),
                configFactory.build(RemoteTaskRunnerConfig.class),
                curatorFramework,
                new PathChildrenCache(curatorFramework, indexerZkConfig.getAnnouncementPath(), true),
                retryScheduledExec,
                new RetryPolicyFactory(
                    configFactory.buildWithReplacements(
                        RetryPolicyConfig.class,
                        ImmutableMap.of("base_path", "druid.indexing")
                    )
                ),
                configManager.watch(WorkerSetupData.CONFIG_KEY, WorkerSetupData.class),
                httpClient
            );
          }
        };

      } else if (config.getRunnerImpl().equals("local")) {
        taskRunnerFactory = new TaskRunnerFactory()
        {
          @Override
          public TaskRunner build()
          {
            final ExecutorService runnerExec = Executors.newFixedThreadPool(config.getNumLocalThreads());
            return new ForkingTaskRunner(
                configFactory.build(ForkingTaskRunnerConfig.class),
                props,
                persistentTaskLogs,
                runnerExec,
                getJsonMapper()
            );
          }
        };
      } else {
        throw new ISE("Invalid runner implementation: %s", config.getRunnerImpl());
      }
    }
  }

  private void initializeResourceManagement(final JacksonConfigManager configManager)
  {
    if (resourceManagementSchedulerFactory == null) {
      resourceManagementSchedulerFactory = new ResourceManagementSchedulerFactory()
      {
        @Override
        public ResourceManagementScheduler build(TaskRunner runner)
        {
          final ScheduledExecutorService scalingScheduledExec = Executors.newScheduledThreadPool(
              1,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("ScalingExec--%d")
                  .build()
          );
          final AtomicReference<WorkerSetupData> workerSetupData = configManager.watch(
              WorkerSetupData.CONFIG_KEY, WorkerSetupData.class
          );

          AutoScalingStrategy strategy;
          if (config.getStrategyImpl().equalsIgnoreCase("ec2")) {
            strategy = new EC2AutoScalingStrategy(
                getJsonMapper(),
                new AmazonEC2Client(
                    new BasicAWSCredentials(
                        PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
                        PropUtils.getProperty(props, "com.metamx.aws.secretKey")
                    )
                ),
                configFactory.build(EC2AutoScalingStrategyConfig.class),
                workerSetupData
            );
          } else if (config.getStrategyImpl().equalsIgnoreCase("noop")) {
            strategy = new NoopAutoScalingStrategy();
          } else {
            throw new ISE("Invalid strategy implementation: %s", config.getStrategyImpl());
          }

          return new ResourceManagementScheduler(
              runner,
              new SimpleResourceManagementStrategy(
                  strategy,
                  configFactory.build(SimpleResourceManagmentConfig.class),
                  workerSetupData
              ),
              configFactory.build(ResourceManagementSchedulerConfig.class),
              scalingScheduledExec
          );
        }
      };
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

    public IndexerCoordinatorNode build()
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

      return new IndexerCoordinatorNode(props, lifecycle, jsonMapper, configFactory);
    }
  }
}
