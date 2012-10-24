package com.metamx.druid.realtime;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.ClientConfig;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.MutableServerView;
import com.metamx.druid.client.OnlyNewSegmentWatcherServerView;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.RequestLogger;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.initialization.ZkClientConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.DefaultQueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
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
import com.metamx.phonebook.PhoneBook;
import org.I0Itec.zkclient.ZkClient;
import org.codehaus.jackson.map.BeanProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.type.TypeReference;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class RealtimeNode
{
  private static final Logger log = new Logger(RealtimeNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Lifecycle lifecycle;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private final Map<String, Object> injectablesMap = Maps.newLinkedHashMap();

  private PhoneBook phoneBook = null;
  private ServiceEmitter emitter = null;
  private ServerView view = null;
  private MetadataUpdater metadataUpdater = null;
  private QueryRunnerFactoryConglomerate conglomerate = null;
  private SegmentPusher segmentPusher = null;
  private List<FireDepartment> fireDepartments = null;
  private List<Monitor> monitors = null;
  private Server server = null;

  private boolean initialized = false;

  public RealtimeNode(
    ObjectMapper jsonMapper,
    ObjectMapper smileMapper,
    Lifecycle lifecycle,
    Properties props,
    ConfigurationObjectFactory configFactory
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.lifecycle = lifecycle;
    this.props = props;
    this.configFactory = configFactory;
  }

  public RealtimeNode setPhoneBook(PhoneBook phoneBook)
  {
    this.phoneBook = phoneBook;
    return this;
  }

  public RealtimeNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public RealtimeNode setView(ServerView view)
  {
    this.view = view;
    return this;
  }

  public RealtimeNode setMetadataUpdater(MetadataUpdater metadataUpdater)
  {
    this.metadataUpdater = metadataUpdater;
    return this;
  }

  public RealtimeNode setConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
    return this;
  }

  public RealtimeNode setSegmentPusher(SegmentPusher segmentPusher)
  {
    this.segmentPusher = segmentPusher;
    return this;
  }

  public RealtimeNode setFireDepartments(List<FireDepartment> fireDepartments)
  {
    this.fireDepartments = fireDepartments;
    return this;
  }

  public RealtimeNode setMonitors(List<Monitor> monitors)
  {
    this.monitors = Lists.newArrayList(monitors);
    return this;
  }

  public void setServer(Server server)
  {
    this.server = server;
  }

  public RealtimeNode registerJacksonInjectable(String name, Object object)
  {
    injectablesMap.put(name, object);
    return this;
  }

  public RealtimeNode registerJacksonSubtype(Class<?>... clazzes)
  {
    jsonMapper.registerSubtypes(clazzes);
    return this;
  }

  public RealtimeNode registerJacksonSubtype(NamedType... namedTypes)
  {
    jsonMapper.registerSubtypes(namedTypes);
    return this;
  }

  private void init() throws Exception
  {
    if (phoneBook == null) {
      final ZkClient zkClient = Initialization.makeZkClient(configFactory.build(ZkClientConfig.class), lifecycle);
      phoneBook = Initialization.createYellowPages(
          jsonMapper,
          zkClient,
          "Realtime-ZKYP--%s",
          lifecycle
      );
    }

    initializeEmitter();
    initializeView();
    initializeMetadataUpdater();
    initializeQueryRunnerFactoryConglomerate();
    initializeSegmentPusher();
    initializeMonitors();
    initializeServer();
    initializeJacksonInjectables();
    initializeFireDepartments();
    monitors.add(new RealtimeMetricsMonitor(fireDepartments));

    final RealtimeManager realtimeManager = new RealtimeManager(fireDepartments, conglomerate);
    lifecycle.addManagedInstance(realtimeManager);

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(monitorScheduler);

    final RequestLogger requestLogger = Initialization.makeRequestLogger(globalScheduledExec, props);
    lifecycle.addManagedInstance(requestLogger);

    final Context v2Druid = new Context(server, "/druid/v2", Context.SESSIONS);
    v2Druid.addServlet(new ServletHolder(new StatusServlet()), "/status");
    v2Druid.addServlet(
        new ServletHolder(new QueryServlet(jsonMapper, smileMapper, realtimeManager, emitter, requestLogger)),
        "/*"
    );

    initialized = true;
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (! initialized) {
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

  private void initializeJacksonInjectables()
  {
    final Map<String, Object> injectables = Maps.newHashMap();

    for (Map.Entry<String, Object> entry : injectablesMap.entrySet()) {
      injectables.put(entry.getKey(), entry.getValue());
    }

    injectables.put("queryRunnerFactoryConglomerate", conglomerate);
    injectables.put("segmentPusher", segmentPusher);
    injectables.put("metadataUpdater", metadataUpdater);
    injectables.put("serverView", view);
    injectables.put("serviceEmitter", emitter);

    jsonMapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            return injectables.get(valueId);
          }
        }
    );
  }

  private void initializeMonitors()
  {
    if (monitors == null) {
      monitors = Lists.newArrayList();
      monitors.add(new JvmMonitor());
      monitors.add(new SysMonitor());
    }
  }

  private void initializeFireDepartments() throws IOException
  {
    if (fireDepartments == null) {
      fireDepartments = jsonMapper.readValue(
          new File(PropUtils.getProperty(props, "druid.realtime.specFile")),
          new TypeReference<List<FireDepartment>>(){}
      );
    }
  }

  private void initializeSegmentPusher() throws S3ServiceException
  {
    if (segmentPusher == null) {
      final RestS3Service s3Client = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
              PropUtils.getProperty(props, "com.metamx.aws.secretKey")
          )
      );

      segmentPusher = new S3SegmentPusher(s3Client, configFactory.build(S3SegmentPusherConfig.class), jsonMapper);
    }
  }

  private void initializeQueryRunnerFactoryConglomerate()
  {
    if (conglomerate == null) {
      StupidPool<ByteBuffer> computationBufferPool = ServerInit.makeComputeScratchPool(
          PropUtils.getPropertyAsInt(props, "druid.computation.buffer.size", 1024 * 1024 * 1024)
      );
      conglomerate = new DefaultQueryRunnerFactoryConglomerate(
          ServerInit.initDefaultQueryTypes(configFactory, computationBufferPool)
      );
    }
  }

  private void initializeMetadataUpdater()
  {
    if (metadataUpdater == null) {
      metadataUpdater = new MetadataUpdater(
          jsonMapper,
          configFactory.build(MetadataUpdaterConfig.class),
          phoneBook,
          new DbConnector(configFactory.build(DbConnectorConfig.class)).getDBI()
      );
      lifecycle.addManagedInstance(metadataUpdater);
    }
  }

  private void initializeView()
  {
    if (view == null) {
      final ClientConfig clientConfig = configFactory.build(ClientConfig.class);
      final MutableServerView view = new OnlyNewSegmentWatcherServerView();
      final ClientInventoryManager clientInventoryManager = new ClientInventoryManager(
          clientConfig.getClientInventoryManagerConfig(),
          phoneBook,
          view
      );
      lifecycle.addManagedInstance(clientInventoryManager);

      this.view = view;
    }
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

    public RealtimeNode build()
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

      return new RealtimeNode(jsonMapper, smileMapper, lifecycle, props, configFactory);
    }
  }
}