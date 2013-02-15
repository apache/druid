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
import com.metamx.druid.client.ClientConfig;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.MutableServerView;
import com.metamx.druid.client.OnlyNewSegmentWatcherServerView;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.S3SegmentPusher;
import com.metamx.druid.loading.S3SegmentPusherConfig;
import com.metamx.druid.loading.SegmentPusher;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.Monitor;






import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
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

  private MetadataUpdater metadataUpdater = null;
  private SegmentPusher segmentPusher = null;
  private List<FireDepartment> fireDepartments = null;
  private ServerView view = null;

  private boolean initialized = false;

  public RealtimeNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }

  public RealtimeNode setView(ServerView view)
  {
    Preconditions.checkState(this.view == null, "Cannot set view once it has already been set.");
    this.view = view;
    return this;
  }

  public RealtimeNode setMetadataUpdater(MetadataUpdater metadataUpdater)
  {
    Preconditions.checkState(this.metadataUpdater == null, "Cannot set metadataUpdater once it has already been set.");
    this.metadataUpdater = metadataUpdater;
    return this;
  }

  public RealtimeNode setSegmentPusher(SegmentPusher segmentPusher)
  {
    Preconditions.checkState(this.segmentPusher == null, "Cannot set segmentPusher once it has already been set.");
    this.segmentPusher = segmentPusher;
    return this;
  }

  public RealtimeNode setFireDepartments(List<FireDepartment> fireDepartments)
  {
    Preconditions.checkState(this.fireDepartments == null, "Cannot set fireDepartments once it has already been set.");
    this.fireDepartments = fireDepartments;
    return this;
  }

  public RealtimeNode registerJacksonInjectable(String name, Object object)
  {
    Preconditions.checkState(injectablesMap.containsKey(name), "Already registered jackson object[%s]", name);
    injectablesMap.put(name, object);
    return this;
  }

  public MetadataUpdater getMetadataUpdater()
  {
    initializeMetadataUpdater();
    return metadataUpdater;
  }

  public SegmentPusher getSegmentPusher()
  {
    initializeSegmentPusher();
    return segmentPusher;
  }

  public List<FireDepartment> getFireDepartments()
  {
    initializeFireDepartments();
    return fireDepartments;
  }

  public ServerView getView()
  {
    initializeView();
    return view;
  }

  protected void doInit() throws Exception
  {
    initializeView();
    initializeMetadataUpdater();
    initializeSegmentPusher();
    initializeJacksonInjectables();

    initializeFireDepartments();

    final Lifecycle lifecycle = getLifecycle();
    final ServiceEmitter emitter = getEmitter();
    final QueryRunnerFactoryConglomerate conglomerate = getConglomerate();
    final List<Monitor> monitors = getMonitors();

    monitors.add(new RealtimeMetricsMonitor(fireDepartments));

    final RealtimeManager realtimeManager = new RealtimeManager(fireDepartments, conglomerate);
    lifecycle.addManagedInstance(realtimeManager);

    startMonitoring(monitors);

    final Context v2Druid = new Context(getServer(), "/druid/v2", Context.SESSIONS);
    v2Druid.addServlet(new ServletHolder(new StatusServlet()), "/status");
    v2Druid.addServlet(
        new ServletHolder(
            new QueryServlet(getJsonMapper(), getSmileMapper(), realtimeManager, emitter, getRequestLogger())
        ),
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
    injectables.put("segmentPusher", segmentPusher);
    injectables.put("metadataUpdater", metadataUpdater);
    injectables.put("serverView", view);
    injectables.put("serviceEmitter", getEmitter());

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
        fireDepartments = getJsonMapper().readValue(
            new File(PropUtils.getProperty(getProps(), "druid.realtime.specFile")),
            new TypeReference<List<FireDepartment>>(){}
        );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void initializeSegmentPusher()
  {
    if (segmentPusher == null) {
      final Properties props = getProps();
      final RestS3Service s3Client;
      try {
        s3Client = new RestS3Service(
            new AWSCredentials(
                PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
                PropUtils.getProperty(props, "com.metamx.aws.secretKey")
            )
        );
      }
      catch (S3ServiceException e) {
        throw Throwables.propagate(e);
      }

      segmentPusher = new S3SegmentPusher(s3Client, getConfigFactory().build(S3SegmentPusherConfig.class), getJsonMapper());
    }
  }

  protected void initializeMetadataUpdater()
  {
    if (metadataUpdater == null) {
      metadataUpdater = new MetadataUpdater(
          getJsonMapper(),
          getConfigFactory().build(MetadataUpdaterConfig.class),
          getPhoneBook(),
          new DbConnector(getConfigFactory().build(DbConnectorConfig.class)).getDBI()
      );
      getLifecycle().addManagedInstance(metadataUpdater);
    }
  }

  private void initializeView()
  {
    if (view == null) {
      final MutableServerView view = new OnlyNewSegmentWatcherServerView();
      final ClientInventoryManager clientInventoryManager = new ClientInventoryManager(
          getConfigFactory().build(ClientConfig.class),
          getPhoneBook(),
          view
      );
      getLifecycle().addManagedInstance(clientInventoryManager);

      this.view = view;
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