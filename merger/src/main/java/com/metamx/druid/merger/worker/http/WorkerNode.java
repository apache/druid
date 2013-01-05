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

import com.google.common.collect.Lists;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.RegisteringNode;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.CuratorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.worker.TaskMonitor;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.druid.merger.worker.WorkerCuratorCoordinator;
import com.metamx.druid.merger.worker.config.WorkerConfig;
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
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerNode extends RegisteringNode
{
  private static final Logger log = new Logger(WorkerNode.class);

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
  private IndexerCoordinatorConfig coordinatorConfig = null; // FIXME needed for task toolbox, but shouldn't be
  private WorkerConfig workerConfig = null;
  private TaskToolbox taskToolbox = null;
  private CuratorFramework curatorFramework = null;
  private WorkerCuratorCoordinator workerCuratorCoordinator = null;
  private TaskMonitor taskMonitor = null;
  private Server server = null;

  private boolean initialized = false;

  public WorkerNode(
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

  public WorkerNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public WorkerNode setTaskToolbox(TaskToolbox taskToolbox)
  {
    this.taskToolbox = taskToolbox;
    return this;
  }

  public WorkerNode setCuratorFramework(CuratorFramework curatorFramework)
  {
    this.curatorFramework = curatorFramework;
    return this;
  }

  public WorkerNode setWorkerCuratorCoordinator(WorkerCuratorCoordinator workerCuratorCoordinator)
  {
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    return this;
  }

  public WorkerNode setTaskMonitor(TaskMonitor taskMonitor)
  {
    this.taskMonitor = taskMonitor;
    return this;
  }

  public void init() throws Exception
  {
    initializeEmitter();
    initializeMonitors();
    initializeMergerConfig();
    initializeTaskToolbox();
    initializeJacksonInjections();
    initializeJacksonSubtypes();
    initializeCuratorFramework();
    initializeCuratorCoordinator();
    initializeTaskMonitor();
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

    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/mmx/*");
    root.addFilter(GuiceFilter.class, "/mmx/indexer/worker/v1/*", 0);
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

  private void initializeMergerConfig()
  {
    if (coordinatorConfig == null) {
      coordinatorConfig = configFactory.build(IndexerCoordinatorConfig.class);
    }

    if (workerConfig == null) {
      workerConfig = configFactory.build(WorkerConfig.class);
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
      taskToolbox = new TaskToolbox(coordinatorConfig, emitter, s3Client, segmentPusher, jsonMapper);
    }
  }

  public void initializeCuratorFramework() throws IOException
  {
    final CuratorConfig curatorConfig = configFactory.build(CuratorConfig.class);
    curatorFramework = Initialization.makeCuratorFrameworkClient(
        curatorConfig,
        lifecycle
    );
  }

  public void initializeCuratorCoordinator()
  {
    if (workerCuratorCoordinator == null) {
      workerCuratorCoordinator = new WorkerCuratorCoordinator(
          jsonMapper,
          configFactory.build(IndexerZkConfig.class),
          curatorFramework,
          new Worker(workerConfig)
      );
      lifecycle.addManagedInstance(workerCuratorCoordinator);
    }
  }

  public void initializeTaskMonitor()
  {
    if (taskMonitor == null) {
      final ExecutorService workerExec = Executors.newFixedThreadPool(workerConfig.getNumThreads());
      final PathChildrenCache pathChildrenCache = new PathChildrenCache(
          curatorFramework,
          workerCuratorCoordinator.getTaskPathForWorker(),
          false
      );
      taskMonitor = new TaskMonitor(
          pathChildrenCache,
          curatorFramework,
          workerCuratorCoordinator,
          taskToolbox,
          workerExec
      );
      lifecycle.addManagedInstance(taskMonitor);
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

      return new WorkerNode(jsonMapper, lifecycle, props, configFactory);
    }
  }
}
