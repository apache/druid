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

package com.metamx.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.metamx.druid.BaseServerNode;
import com.metamx.druid.curator.discovery.CuratorServiceAnnouncer;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.curator.discovery.ServiceInstanceFactory;
import com.metamx.druid.http.GuiceServletConfig;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.indexing.common.RetryPolicyFactory;
import com.metamx.druid.indexing.common.TaskToolboxFactory;
import com.metamx.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import com.metamx.druid.indexing.common.config.RetryPolicyConfig;
import com.metamx.druid.indexing.common.config.TaskConfig;
import com.metamx.druid.indexing.common.index.ChatHandlerProvider;
import com.metamx.druid.indexing.common.index.EventReceiverFirehoseFactory;
import com.metamx.druid.indexing.common.index.EventReceivingChatHandlerProvider;
import com.metamx.druid.indexing.common.index.NoopChatHandlerProvider;
import com.metamx.druid.indexing.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.indexing.coordinator.ThreadPoolTaskRunner;
import com.metamx.druid.indexing.worker.config.ChatHandlerProviderConfig;
import com.metamx.druid.indexing.worker.config.WorkerConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.S3DataSegmentKiller;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceProvider;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class ExecutorNode extends BaseServerNode<ExecutorNode>
{
  private static final EmittingLogger log = new EmittingLogger(ExecutorNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Lifecycle lifecycle;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;
  private final ExecutorLifecycleFactory executorLifecycleFactory;

  private RestS3Service s3Service = null;
  private MonitorScheduler monitorScheduler = null;
  private HttpClient httpClient = null;
  private TaskConfig taskConfig = null;
  private WorkerConfig workerConfig = null;
  private DataSegmentPusher segmentPusher = null;
  private TaskToolboxFactory taskToolboxFactory = null;
  private ServiceDiscovery serviceDiscovery = null;
  private ServiceAnnouncer serviceAnnouncer = null;
  private ServiceProvider coordinatorServiceProvider = null;
  private Server server = null;
  private ThreadPoolTaskRunner taskRunner = null;
  private ExecutorLifecycle executorLifecycle = null;
  private ChatHandlerProvider chatHandlerProvider = null;

  public ExecutorNode(
      String nodeType,
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory,
      ExecutorLifecycleFactory executorLifecycleFactory
  )
  {
    super(nodeType, log, props, lifecycle, jsonMapper, smileMapper, configFactory);

    this.lifecycle = lifecycle;
    this.props = props;
    this.configFactory = configFactory;
    this.executorLifecycleFactory = executorLifecycleFactory;
  }

  @Override
  public void doInit() throws Exception
  {
    initializeHttpClient();
    initializeS3Service();
    initializeMergerConfig();
    initializeServiceDiscovery();
    initializeDataSegmentPusher();
    initializeMonitorScheduler();
    initializeTaskToolbox();
    initializeTaskRunner();
    initializeChatHandlerProvider();
    initializeJacksonInjections();
    initializeJacksonSubtypes();
    initializeServer();

    executorLifecycle = executorLifecycleFactory.build(taskRunner, getJsonMapper());
    lifecycle.addManagedInstance(executorLifecycle);

    final Injector injector = Guice.createInjector(
        new ExecutorServletModule(
            getJsonMapper(),
            chatHandlerProvider
        )
    );
    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addEventListener(new GuiceServletConfig(injector));
    root.addFilter(GuiceFilter.class, "/druid/worker/v1/*", 0);
    root.addServlet(
        new ServletHolder(
            new QueryServlet(getJsonMapper(), getSmileMapper(), taskRunner, getEmitter(), getRequestLogger())
        ),
        "/druid/v2/*"
    );
  }

  private void initializeMonitorScheduler()
  {
    if (monitorScheduler == null)
    {
      final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
      final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
      this.monitorScheduler = new MonitorScheduler(
          configFactory.build(MonitorSchedulerConfig.class), globalScheduledExec, getEmitter(), ImmutableList.<Monitor>of()
      );
      lifecycle.addManagedInstance(monitorScheduler);
    }
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    init();
    lifecycle.start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    lifecycle.stop();
  }

  public void join()
  {
    executorLifecycle.join();
  }

  public ThreadPoolTaskRunner getTaskRunner()
  {
    return taskRunner;
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

    injectables.addValue("s3Client", s3Service)
               .addValue("segmentPusher", segmentPusher)
               .addValue("chatHandlerProvider", chatHandlerProvider);

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
          HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
      );
    }
  }

  private void initializeS3Service() throws S3ServiceException
  {
    if (s3Service == null) {
      s3Service = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
              PropUtils.getProperty(props, "com.metamx.aws.secretKey")
          )
      );
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

  public void initializeDataSegmentPusher()
  {
    if (segmentPusher == null) {
      segmentPusher = ServerInit.getSegmentPusher(props, configFactory, getJsonMapper());
    }
  }

  public void initializeTaskToolbox() throws S3ServiceException
  {
    if (taskToolboxFactory == null) {
      final DataSegmentKiller dataSegmentKiller = new S3DataSegmentKiller(s3Service);
      taskToolboxFactory = new TaskToolboxFactory(
          taskConfig,
          new RemoteTaskActionClientFactory(
              httpClient,
              coordinatorServiceProvider,
              new RetryPolicyFactory(
                  configFactory.buildWithReplacements(
                      RetryPolicyConfig.class,
                      ImmutableMap.of("base_path", "druid.worker.taskActionClient")
                  )
              ),
              getJsonMapper()
          ),
          getEmitter(),
          s3Service,
          segmentPusher,
          dataSegmentKiller,
          getAnnouncer(),
          getServerView(),
          getConglomerate(),
          getQueryExecutorService(),
          monitorScheduler,
          getJsonMapper()
      );
    }
  }

  public void initializeServiceDiscovery() throws Exception
  {
    final ServiceDiscoveryConfig config = configFactory.build(ServiceDiscoveryConfig.class);
    if (serviceDiscovery == null) {
      final CuratorFramework serviceDiscoveryCuratorFramework = Initialization.makeCuratorFramework(config, lifecycle);
      this.serviceDiscovery = Initialization.makeServiceDiscoveryClient(
          serviceDiscoveryCuratorFramework, config, lifecycle
      );
    }
    if (serviceAnnouncer == null) {
      final ServiceInstanceFactory instanceFactory = Initialization.makeServiceInstanceFactory(config);
      this.serviceAnnouncer = new CuratorServiceAnnouncer(serviceDiscovery, instanceFactory);
    }
    if (coordinatorServiceProvider == null) {
      this.coordinatorServiceProvider = Initialization.makeServiceProvider(
          workerConfig.getMasterService(),
          serviceDiscovery,
          lifecycle
      );
    }
  }

  public void initializeTaskRunner()
  {
    if (taskRunner == null) {
      this.taskRunner = lifecycle.addManagedInstance(
          new ThreadPoolTaskRunner(
              taskToolboxFactory,
              Executors.newSingleThreadExecutor(
                  new ThreadFactoryBuilder()
                      .setNameFormat("task-runner-%d")
                      .build()
              )
          )
      );
    }
  }

  public void initializeChatHandlerProvider()
  {
    if (chatHandlerProvider == null) {
      final ChatHandlerProviderConfig config = configFactory.build(ChatHandlerProviderConfig.class);
      if (config.getServiceFormat() == null) {
        log.info("ChatHandlerProvider: Using NoopChatHandlerProvider. Good luck finding your firehoses!");
        this.chatHandlerProvider = new NoopChatHandlerProvider();
      } else {
        this.chatHandlerProvider = new EventReceivingChatHandlerProvider(
            config,
            serviceAnnouncer
        );
      }
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

    public ExecutorNode build(String nodeType, ExecutorLifecycleFactory executorLifecycleFactory)
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

      return new ExecutorNode(
          nodeType,
          props,
          lifecycle,
          jsonMapper,
          smileMapper,
          configFactory,
          executorLifecycleFactory
      );
    }
  }
}
