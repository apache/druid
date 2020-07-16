/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.initialization.jetty;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.RemoteChatHandler;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.TLSServerConfig;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Properties;

/**
 */
public class CliIndexerServerModule implements Module
{
  private static final String SERVER_HTTP_NUM_THREADS_PROPERTY = "druid.server.http.numThreads";
  private final Properties properties;

  public CliIndexerServerModule(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public void configure(Binder binder)
  {
    Jerseys.addResource(binder, ChatHandlerResource.class);
    LifecycleModule.register(binder, ChatHandlerResource.class);

    // Use an equal number of threads for chat handler and non-chat handler requests.
    int serverHttpNumThreads;
    if (properties.getProperty(SERVER_HTTP_NUM_THREADS_PROPERTY) == null) {
      serverHttpNumThreads = ServerConfig.getDefaultNumThreads();
    } else {
      serverHttpNumThreads = Integer.parseInt(properties.getProperty(SERVER_HTTP_NUM_THREADS_PROPERTY));
    }

    JettyBindings.addQosFilter(
        binder,
        "/druid/worker/v1/chat/*",
        serverHttpNumThreads
    );

    String[] notChatPaths = new String[]{
        "/druid/v2/*", // QueryResource
        "/status/*", // StatusResource
        "/druid-internal/*", // SegmentListerResource, TaskManagementResource
        "/druid/worker/v1/enable", // WorkerResource
        "/druid/worker/v1/disable", // WorkerResource
        "/druid/worker/v1/enabled", // WorkerResource
        "/druid/worker/v1/tasks", // WorkerResource
        "/druid/worker/v1/task/*", // WorkerResource
        "/druid/v1/lookups/*", // LookupIntrospectionResource
        "/druid-ext/*" // basic-security
    };
    JettyBindings.addQosFilter(
        binder,
        notChatPaths,
        serverHttpNumThreads
    );

    // Be aware that lookups have a 2 maxRequest QoS filter as well.

    Multibinder.newSetBinder(binder, ServletFilterHolder.class).addBinding().to(TaskIdResponseHeaderFilterHolder.class);

    /**
     * We bind {@link DruidNode} annotated with {@link RemoteChatHandler} to {@literal @}{@link Self} {@link DruidNode}
     * so that same Jetty Server is used for querying as well as ingestion.
     */
    binder.bind(DruidNode.class).annotatedWith(RemoteChatHandler.class).to(Key.get(DruidNode.class, Self.class));
    binder.bind(ServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(ServerConfig.class));
    binder.bind(TLSServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(TLSServerConfig.class));
  }

  @Provides
  @LazySingleton
  public TaskIdResponseHeaderFilterHolder taskIdResponseHeaderFilterHolderBuilder(
      final DataSourceTaskIdHolder taskIdHolder
  )
  {
    return new TaskIdResponseHeaderFilterHolder("/druid/worker/v1/chat/*", taskIdHolder.getTaskId());
  }

  @Provides
  @LazySingleton
  @RemoteChatHandler
  public Server getServer(
      Injector injector,
      Lifecycle lifecycle,
      @RemoteChatHandler DruidNode node,
      @RemoteChatHandler ServerConfig config,
      @RemoteChatHandler TLSServerConfig TLSServerConfig
  )
  {
    return JettyServerModule.makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        makeAdjustedServerConfig(config),
        TLSServerConfig,
        injector.getExistingBinding(Key.get(SslContextFactory.Server.class)),
        injector.getInstance(TLSCertificateChecker.class)
    );
  }

  /**
   * Adjusts the ServerConfig such that we double the number of configured HTTP threads,
   * with one half allocated using QoS to chat handler requests, and the other half for other requests.
   *
   * 2 dedicated threads are added for lookup listening, which also has a QoS filter applied.
   */
  public ServerConfig makeAdjustedServerConfig(ServerConfig oldConfig)
  {
    return new ServerConfig(
        (oldConfig.getNumThreads() * 2) + LookupModule.LOOKUP_LISTENER_QOS_MAX_REQUESTS,
        oldConfig.getQueueSize(),
        oldConfig.isEnableRequestLimit(),
        oldConfig.getMaxIdleTime(),
        oldConfig.getDefaultQueryTimeout(),
        oldConfig.getMaxScatterGatherBytes(),
        oldConfig.getMaxSubqueryRows(),
        oldConfig.getMaxQueryTimeout(),
        oldConfig.getMaxRequestHeaderSize(),
        oldConfig.getGracefulShutdownTimeout(),
        oldConfig.getUnannouncePropagationDelay(),
        oldConfig.getInflateBufferSize(),
        oldConfig.getCompressionLevel(),
        oldConfig.isEnableForwardedRequestCustomizer(),
        oldConfig.getAllowedHttpMethods()
    );
  }
}
