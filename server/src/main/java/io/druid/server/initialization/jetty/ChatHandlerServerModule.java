/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.initialization.jetty;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.RemoteChatHandler;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.firehose.ChatHandlerResource;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.TLSServerConfig;
import io.druid.server.metrics.DataSourceTaskIdHolder;
import org.eclipse.jetty.server.Server;

import java.util.Properties;

/**
 */
public class ChatHandlerServerModule implements Module
{
  private static final Logger log = new Logger(ChatHandlerServerModule.class);
  private static final String MAX_CHAT_REQUESTS_PROPERTY = "druid.indexer.server.maxChatRequests";
  private static final String CHAT_PORT_PROPERTY = "druid.indexer.task.chathandler.port";

  private final Properties properties;

  public ChatHandlerServerModule(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public void configure(Binder binder)
  {
    Jerseys.addResource(binder, ChatHandlerResource.class);
    LifecycleModule.register(binder, ChatHandlerResource.class);

    if (properties.containsKey(MAX_CHAT_REQUESTS_PROPERTY)) {
      final int maxRequests = Integer.parseInt(properties.getProperty(MAX_CHAT_REQUESTS_PROPERTY));
      JettyBindings.addQosFilter(binder, "/druid/worker/v1/chat/*", maxRequests);
    }

    Multibinder.newSetBinder(binder, ServletFilterHolder.class).addBinding().to(TaskIdResponseHeaderFilterHolder.class);

    /**
     * If "druid.indexer.task.chathandler.port" property is set then we assume that a separate Jetty Server with its
     * own {@link ServerConfig} is required for ingestion apart from the query server otherwise we bind
     * {@link DruidNode} annotated with {@link RemoteChatHandler} to {@literal @}{@link Self} {@link DruidNode}
     * so that same Jetty Server is used for querying as well as ingestion.
     */
    if (properties.containsKey(CHAT_PORT_PROPERTY)) {
      log.info("Spawning separate ingestion server at port [%s]", properties.getProperty(CHAT_PORT_PROPERTY));
      JsonConfigProvider.bind(binder, "druid.indexer.task.chathandler", DruidNode.class, RemoteChatHandler.class);
      JsonConfigProvider.bind(
          binder,
          "druid.indexer.server.chathandler.http",
          ServerConfig.class,
          RemoteChatHandler.class
      );
      JsonConfigProvider.bind(
          binder,
          "druid.indexer.server.chathandler.https",
          TLSServerConfig.class,
          RemoteChatHandler.class
      );
      LifecycleModule.register(binder, Server.class, RemoteChatHandler.class);
    } else {
      binder.bind(DruidNode.class).annotatedWith(RemoteChatHandler.class).to(Key.get(DruidNode.class, Self.class));
      binder.bind(ServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(ServerConfig.class));
      binder.bind(TLSServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(TLSServerConfig.class));
    }
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
    final Server server = JettyServerModule.makeJettyServer(node, config, TLSServerConfig);
    JettyServerModule.initializeServer(injector, lifecycle, server);
    return server;
  }
}
