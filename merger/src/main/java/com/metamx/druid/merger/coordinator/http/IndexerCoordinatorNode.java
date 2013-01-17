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
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import com.metamx.druid.loading.S3SegmentKiller;
import com.metamx.druid.loading.SegmentKiller;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.merger.coordinator.DbTaskStorage;
import com.metamx.druid.merger.coordinator.LocalTaskRunner;
import com.metamx.druid.merger.coordinator.LocalTaskStorage;
import com.metamx.druid.merger.coordinator.MergerDBCoordinator;
import com.metamx.druid.merger.coordinator.RemoteTaskRunner;
import com.metamx.druid.merger.coordinator.RetryPolicyFactory;
import com.metamx.druid.merger.coordinator.TaskMaster;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.TaskRunner;
import com.metamx.druid.merger.coordinator.TaskRunnerFactory;
import com.metamx.druid.merger.coordinator.TaskStorage;
import com.metamx.druid.merger.coordinator.config.EC2AutoScalingStrategyConfig;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.config.IndexerDbConnectorConfig;
import com.metamx.druid.merger.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.config.RetryPolicyConfig;
import com.metamx.druid.merger.coordinator.scaling.EC2AutoScalingStrategy;
import com.metamx.druid.merger.coordinator.scaling.NoopScalingStrategy;
import com.metamx.druid.merger.coordinator.scaling.ScalingStrategy;
import com.metamx.druid.loading.S3SegmentPusher;
import com.metamx.druid.loading.S3SegmentPusherConfig;
import com.metamx.druid.loading.SegmentPusher;
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
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class IndexerCoordinatorNode extends RegisteringNode
{
  private static final Logger log = new Logger(IndexerCoordinatorNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final ObjectMapper jsonMapper;
  private final Lifecycle lifecycle;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private List<Monitor> monitors = null;
  private ServiceEmitter emitter = null;
  private DbConnectorConfig dbConnectorConfig = null;
  private DBI dbi = null;
  private IndexerCoordinatorConfig config = null;
  private TaskToolbox taskToolbox = null;
  private MergerDBCoordinator mergerDBCoordinator = null;
  private TaskStorage taskStorage = null;
  private TaskQueue taskQueue = null;
  private CuratorFramework curatorFramework = null;
  private ScheduledExecutorFactory scheduledExecutorFactory = null;
  private IndexerZkConfig indexerZkConfig;
  private TaskRunnerFactory taskRunnerFactory = null;
  private TaskMaster taskMaster = null;
  private Server server = null;

  private boolean initialized = false;

  public IndexerCoordinatorNode(
      ObjectMapper jsonMapper,
      Lifecycle lifecycle,
      Properties props,
      ConfigurationObjectFactory configFactory
  )
  {
    super(Arrays.asList(jsonMapper));

    this.jsonMapper = jsonMapper;
    this.lifecycle = lifecycle;
    this.props = props;
    this.configFactory = configFactory;
  }

  public IndexerCoordinatorNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public void setMergerDBCoordinator(MergerDBCoordinator mergerDBCoordinator)
  {
    this.mergerDBCoordinator = mergerDBCoordinator;
  }

  public void setTaskQueue(TaskQueue taskQueue)
  {
    this.taskQueue = taskQueue;
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

  public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory)
  {
    this.taskRunnerFactory = taskRunnerFactory;
  }

  public void init() throws Exception
  {
    scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);

    initializeEmitter();
    initializeMonitors();
    initializeDB();
    initializeIndexerCoordinatorConfig();
    initializeMergeDBCoordinator();
    initializeTaskToolbox();
    initializeTaskStorage();
    initializeTaskQueue();
    initializeJacksonInjections();
    initializeJacksonSubtypes();
    initializeCurator();
    initializeIndexerZkConfig();
    initializeTaskRunnerFactory();
    initializeTaskMaster();
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
            jsonMapper,
            config,
            emitter,
            taskQueue
        )
    );

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
                    return taskMaster.isLeading();
                  }

                  @Override
                  public URL getRedirectURL(String queryString, String requestURI)
                  {
                    try {
                      return new URL(
                          String.format(
                              "http://%s%s",
                              taskMaster.getLeader(),
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

  private void initializeTaskMaster()
  {
    if (taskMaster == null) {
      final ServiceDiscoveryConfig serviceDiscoveryConfig = configFactory.build(ServiceDiscoveryConfig.class);
      taskMaster = new TaskMaster(
          taskQueue,
          config,
          serviceDiscoveryConfig,
          mergerDBCoordinator,
          taskRunnerFactory,
          curatorFramework,
          emitter
      );
      lifecycle.addManagedInstance(taskMaster);
    }
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (!initialized) {
      init();
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

  private void initializeJacksonInjections()
  {
    InjectableValues.Std injectables = new InjectableValues.Std();

    injectables.addValue("s3Client", taskToolbox.getS3Client())
               .addValue("segmentPusher", taskToolbox.getSegmentPusher());

    jsonMapper.setInjectableValues(injectables);
  }

  private void initializeJacksonSubtypes()
  {
    jsonMapper.registerSubtypes(StaticS3FirehoseFactory.class);
  }

  private void initializeEmitter()
  {
    if (emitter == null) {
      final HttpClient httpClient = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
      );

      emitter = new ServiceEmitter(
          PropUtils.getProperty(props, "druid.service"),
          PropUtils.getProperty(props, "druid.host"),
          Emitters.create(props, httpClient, jsonMapper, lifecycle)
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

  public void initializeTaskToolbox() throws S3ServiceException
  {
    if (taskToolbox == null) {
      final RestS3Service s3Client = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
              PropUtils.getProperty(props, "com.metamx.aws.secretKey")
          )
      );
      final SegmentPusher segmentPusher = new S3SegmentPusher(
          s3Client,
          configFactory.build(S3SegmentPusherConfig.class),
          jsonMapper
      );
      final SegmentKiller segmentKiller = new S3SegmentKiller(
          s3Client,
          dbi,
          dbConnectorConfig,
          jsonMapper
      );
      taskToolbox = new TaskToolbox(config, emitter, s3Client, segmentPusher, segmentKiller, jsonMapper);
    }
  }

  public void initializeMergeDBCoordinator()
  {
    if (mergerDBCoordinator == null) {
      mergerDBCoordinator = new MergerDBCoordinator(
          jsonMapper,
          dbConnectorConfig,
          dbi
      );
    }
  }

  public void initializeTaskQueue()
  {
    if (taskQueue == null) {
      // Don't start it here. The TaskMaster will handle that when it feels like it.
      taskQueue = new TaskQueue(taskStorage);
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
        taskStorage = new LocalTaskStorage();
      } else if (config.getStorageImpl().equals("db")) {
        final IndexerDbConnectorConfig dbConnectorConfig = configFactory.build(IndexerDbConnectorConfig.class);
        taskStorage = new DbTaskStorage(jsonMapper, dbConnectorConfig, new DbConnector(dbConnectorConfig).getDBI());
      } else {
        throw new ISE("Invalid storage implementation: %s", config.getStorageImpl());
      }
    }
  }

  public void initializeTaskRunnerFactory()
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

            ScalingStrategy strategy;
            if (config.getStrategyImpl().equalsIgnoreCase("ec2")) {
              strategy = new EC2AutoScalingStrategy(
                  jsonMapper,
                  new AmazonEC2Client(
                      new BasicAWSCredentials(
                          PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
                          PropUtils.getProperty(props, "com.metamx.aws.secretKey")
                      )
                  ),
                  configFactory.build(EC2AutoScalingStrategyConfig.class)
              );
            } else if (config.getStrategyImpl().equalsIgnoreCase("noop")) {
              strategy = new NoopScalingStrategy();
            } else {
              throw new ISE("Invalid strategy implementation: %s", config.getStrategyImpl());
            }

            return new RemoteTaskRunner(
                jsonMapper,
                configFactory.build(RemoteTaskRunnerConfig.class),
                curatorFramework,
                new PathChildrenCache(curatorFramework, indexerZkConfig.getAnnouncementPath(), true),
                retryScheduledExec,
                new RetryPolicyFactory(configFactory.build(RetryPolicyConfig.class)),
                strategy
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
            return new LocalTaskRunner(taskToolbox, runnerExec);
          }
        };
      } else {
        throw new ISE("Invalid runner implementation: %s", config.getRunnerImpl());
      }
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

      return new IndexerCoordinatorNode(jsonMapper, lifecycle, props, configFactory);
    }
  }
}
