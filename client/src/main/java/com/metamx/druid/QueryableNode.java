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

package com.metamx.druid;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.http.RequestLogger;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ZkClientConfig;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;
import org.codehaus.jackson.smile.SmileFactory;
import org.mortbay.jetty.Server;
import org.skife.config.ConfigurationObjectFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public abstract class QueryableNode<T extends QueryableNode> extends RegisteringNode
{
  private final Logger log;

  private final Lifecycle lifecycle;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private PhoneBook phoneBook = null;
  private ServiceEmitter emitter = null;
  private List<Monitor> monitors = null;
  private Server server = null;
  private ZkClient zkClient;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private RequestLogger requestLogger;

  private boolean initialized = false;

  public QueryableNode(
      Logger log,
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(Arrays.asList(jsonMapper, smileMapper));

    this.log = log;
    this.configFactory = configFactory;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.lifecycle = lifecycle;
    this.smileMapper = smileMapper;

    Preconditions.checkNotNull(props, "props");
    Preconditions.checkNotNull(lifecycle, "lifecycle");
    Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    Preconditions.checkNotNull(smileMapper, "smileMapper");
    Preconditions.checkNotNull(configFactory, "configFactory");

    Preconditions.checkState(smileMapper.getJsonFactory() instanceof SmileFactory, "smileMapper should use smile.");
  }

  @SuppressWarnings("unchecked")
  public T setZkClient(ZkClient zkClient)
  {
    checkFieldNotSetAndSet("zkClient", zkClient);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setPhoneBook(PhoneBook phoneBook)
  {
    checkFieldNotSetAndSet("phoneBook", phoneBook);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setEmitter(ServiceEmitter emitter)
  {
    checkFieldNotSetAndSet("emitter", emitter);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setMonitors(List<Monitor> monitors)
  {
    checkFieldNotSetAndSet("monitors", monitors);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setServer(Server server)
  {
    checkFieldNotSetAndSet("server", server);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setScheduledExecutorFactory(ScheduledExecutorFactory factory)
  {
    checkFieldNotSetAndSet("scheduledExecutorFactory", factory);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setRequestLogger(RequestLogger requestLogger)
  {
    checkFieldNotSetAndSet("requestLogger", requestLogger);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T registerJacksonSubtype(Class<?>... clazzes)
  {
    jsonMapper.registerSubtypes(clazzes);
    smileMapper.registerSubtypes(clazzes);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T registerJacksonSubtype(NamedType... namedTypes)
  {
    jsonMapper.registerSubtypes(namedTypes);
    smileMapper.registerSubtypes(namedTypes);
    return (T) this;
  }

  public Lifecycle getLifecycle()
  {
    return lifecycle;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public ObjectMapper getSmileMapper()
  {
    return smileMapper;
  }

  public Properties getProps()
  {
    return props;
  }

  public ConfigurationObjectFactory getConfigFactory()
  {
    return configFactory;
  }

  public ZkClient getZkClient()
  {
    initializeZkClient();
    return zkClient;
  }

  public PhoneBook getPhoneBook()
  {
    initializePhoneBook();
    return phoneBook;
  }

  public ServiceEmitter getEmitter()
  {
    initializeEmitter();
    return emitter;
  }

  public List<Monitor> getMonitors()
  {
    initializeMonitors();
    return monitors;
  }

  public Server getServer()
  {
    initializeServer();
    return server;
  }

  public ScheduledExecutorFactory getScheduledExecutorFactory()
  {
    initializeScheduledExecutorFactory();
    return scheduledExecutorFactory;
  }

  public RequestLogger getRequestLogger()
  {
    initializeRequestLogger();
    return requestLogger;
  }

  private void initializeRequestLogger()
  {
    if (requestLogger == null) {
      try {
        setRequestLogger(Initialization.makeRequestLogger(getScheduledExecutorFactory(), getProps()));
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      lifecycle.addManagedInstance(requestLogger);
    }
  }

  private void initializeScheduledExecutorFactory()
  {
    if (scheduledExecutorFactory == null) {
      setScheduledExecutorFactory(ScheduledExecutors.createFactory(getLifecycle()));
    }
  }

  private void initializeZkClient()
  {
    if (zkClient == null) {
      setZkClient(Initialization.makeZkClient(configFactory.build(ZkClientConfig.class), lifecycle));
    }
  }

  private void initializePhoneBook()
  {
    if (phoneBook == null) {
      setPhoneBook(
          Initialization.createPhoneBook(
              jsonMapper,
              getZkClient(),
              "PhoneBook--%s",
              lifecycle
          )
      );
    }
  }

  private void initializeServer()
  {
    if (server == null) {
      setServer(Initialization.makeJettyServer(configFactory.build(ServerConfig.class)));

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

  private void initializeMonitors()
  {
    if (monitors == null) {
      List<Monitor> theMonitors = Lists.newArrayList();
      theMonitors.add(new JvmMonitor());
      if (Boolean.parseBoolean(props.getProperty("druid.monitoring.monitorSystem", "false"))) {
        theMonitors.add(new SysMonitor());
      }

      setMonitors(theMonitors);
    }
  }

  private void initializeEmitter()
  {
    if (emitter == null) {
      final HttpClient httpClient = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1).build(), lifecycle
      );

      setEmitter(
          new ServiceEmitter(
              PropUtils.getProperty(props, "druid.service"),
              PropUtils.getProperty(props, "druid.host"),
              Emitters.create(props, httpClient, jsonMapper, lifecycle)
          )
      );
    }
    EmittingLogger.registerEmitter(emitter);
  }

  protected void init() throws Exception
  {
    doInit();
    initialized = true;
  }

  protected abstract void doInit() throws Exception;

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

  protected ScheduledExecutorService startMonitoring(List<Monitor> monitors)
  {
    final ScheduledExecutorService globalScheduledExec = getScheduledExecutorFactory().create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        getConfigFactory().build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        getEmitter(),
        monitors
    );
    getLifecycle().addManagedInstance(monitorScheduler);
    return globalScheduledExec;
  }

  protected void checkFieldNotSetAndSet(String fieldName, Object value)
  {
    Class<?> theClazz = this.getClass();
    while (theClazz != null && theClazz != Object.class) {
      try {
        final Field field = theClazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        Preconditions.checkState(field.get(this) == null, "Cannot set %s once it has already been set.", fieldName);

        field.set(this, value);
        return;
      }
      catch (NoSuchFieldException e) {
        // Perhaps it is inherited?
        theClazz = theClazz.getSuperclass();
      }
      catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }

    throw new ISE("Unknown field[%s] on class[%s]", fieldName, this.getClass());
  }
}
