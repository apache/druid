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

package com.metamx.druid.realtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.BaseServerNode;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.Monitor;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class RealtimeNode extends BaseServerNode<RealtimeNode>
{
  private static final Logger log = new Logger(RealtimeNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Map<String, Object> injectablesMap = Maps.newLinkedHashMap();

  private SegmentPublisher segmentPublisher = null;
  private DataSegmentPusher dataSegmentPusher = null;
  private List<FireDepartment> fireDepartments = null;

  private boolean initialized = false;

  public RealtimeNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super("realtime", log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }

  public RealtimeNode setSegmentPublisher(SegmentPublisher segmentPublisher)
  {
    checkFieldNotSetAndSet("segmentPublisher", segmentPublisher);
    return this;
  }

  public RealtimeNode setDataSegmentPusher(DataSegmentPusher dataSegmentPusher)
  {
    checkFieldNotSetAndSet("dataSegmentPusher", dataSegmentPusher);
    return this;
  }

  public RealtimeNode setFireDepartments(List<FireDepartment> fireDepartments)
  {
    checkFieldNotSetAndSet("fireDepartments", fireDepartments);
    return this;
  }

  public RealtimeNode registerJacksonInjectable(String name, Object object)
  {
    Preconditions.checkState(injectablesMap.containsKey(name), "Already registered jackson object[%s]", name);
    injectablesMap.put(name, object);
    return this;
  }

  public SegmentPublisher getSegmentPublisher()
  {
    initializeSegmentPublisher();
    return segmentPublisher;
  }

  public DataSegmentPusher getDataSegmentPusher()
  {
    initializeSegmentPusher();
    return dataSegmentPusher;
  }

  public List<FireDepartment> getFireDepartments()
  {
    initializeFireDepartments();
    return fireDepartments;
  }

  protected void doInit() throws Exception
  {
    initializeJacksonInjectables();

    final Lifecycle lifecycle = getLifecycle();
    final ServiceEmitter emitter = getEmitter();
    final QueryRunnerFactoryConglomerate conglomerate = getConglomerate();
    final List<Monitor> monitors = getMonitors();
    final List<FireDepartment> departments = getFireDepartments();

    monitors.add(new RealtimeMetricsMonitor(departments));

    final RealtimeManager realtimeManager = new RealtimeManager(departments, conglomerate);
    lifecycle.addManagedInstance(realtimeManager);

    startMonitoring(monitors);

    final Context root = new Context(getServer(), "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(
        new ServletHolder(
            new QueryServlet(getJsonMapper(), getSmileMapper(), realtimeManager, emitter, getRequestLogger())
        ),
        "/druid/v2/*"
    );

    initialized = true;
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (! initialized) {
      init();
    }

    getLifecycle().start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    getLifecycle().stop();
  }

  protected void initializeJacksonInjectables()
  {
    final Map<String, Object> injectables = Maps.newHashMap();

    for (Map.Entry<String, Object> entry : injectablesMap.entrySet()) {
      injectables.put(entry.getKey(), entry.getValue());
    }

    injectables.put("queryRunnerFactoryConglomerate", getConglomerate());
    injectables.put("segmentPusher", getDataSegmentPusher());
    injectables.put("segmentAnnouncer", getAnnouncer());
    injectables.put("segmentPublisher", getSegmentPublisher());
    injectables.put("serverView", getServerView());
    injectables.put("serviceEmitter", getEmitter());
    injectables.put("queryExecutorService", getQueryExecutorService());

    getJsonMapper().setInjectableValues(
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

  private void initializeFireDepartments()
  {
    if (fireDepartments == null) {
      try {
        setFireDepartments(
            getJsonMapper().<List<FireDepartment>>readValue(
                new File(PropUtils.getProperty(getProps(), "druid.realtime.specFile")),
                new TypeReference<List<FireDepartment>>(){}
            )
        );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void initializeSegmentPusher()
  {
    if (dataSegmentPusher == null) {
      dataSegmentPusher = ServerInit.getSegmentPusher(getProps(), getConfigFactory(), getJsonMapper());
    }
  }

  protected void initializeSegmentPublisher()
  {
    if (segmentPublisher == null) {
      final DbSegmentPublisherConfig dbSegmentPublisherConfig = getConfigFactory().build(DbSegmentPublisherConfig.class);
      segmentPublisher = new DbSegmentPublisher(
          getJsonMapper(),
          dbSegmentPublisherConfig,
          new DbConnector(getConfigFactory().build(DbConnectorConfig.class)).getDBI()
      );
      getLifecycle().addManagedInstance(segmentPublisher);
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

      return new RealtimeNode(props, lifecycle, jsonMapper, smileMapper, configFactory);
    }
  }
}
