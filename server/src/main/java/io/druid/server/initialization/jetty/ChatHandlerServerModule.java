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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.RemoteChatHandler;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import org.eclipse.jetty.server.Server;

import java.util.Properties;

/**
 */
public class ChatHandlerServerModule implements Module
{
  private static final Logger log = new Logger(ChatHandlerServerModule.class);

  @Inject
  private Properties properties;

  @Override
  public void configure(Binder binder)
  {
    /** If "druid.indexer.task.chathandler.port" property is set then we assume that a
     * separate Jetty Server with it's own {@link ServerConfig} is required for ingestion apart from the query server
     * otherwise we bind {@link DruidNode} annotated with {@link RemoteChatHandler} to {@literal @}{@link Self} {@link DruidNode}
     * so that same Jetty Server is used for querying as well as ingestion
     */
    if (properties.containsKey("druid.indexer.task.chathandler.port")) {
      log.info("Spawning separate ingestion server at port [%s]", properties.get("druid.indexer.task.chathandler.port"));
      JsonConfigProvider.bind(binder, "druid.indexer.task.chathandler", DruidNode.class, RemoteChatHandler.class);
      JsonConfigProvider.bind(binder, "druid.indexer.server.chathandler.http", ServerConfig.class, RemoteChatHandler.class);
      LifecycleModule.register(binder, Server.class, RemoteChatHandler.class);
    } else {
      binder.bind(DruidNode.class).annotatedWith(RemoteChatHandler.class).to(Key.get(DruidNode.class, Self.class));
      binder.bind(ServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(ServerConfig.class));
    }
  }

  @Provides
  @LazySingleton
  @RemoteChatHandler
  public Server getServer(Injector injector, Lifecycle lifecycle, @RemoteChatHandler DruidNode node, @RemoteChatHandler ServerConfig config)
  {
    final Server server = JettyServerModule.makeJettyServer(node, config);
    JettyServerModule.initializeServer(injector, lifecycle, server);
    return server;
  }
}
