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
public class ChatHandlerServerModule implements Module
{
  private static final String MAX_CHAT_REQUESTS_PROPERTY = "druid.indexer.server.maxChatRequests";
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
      JettyBindings.addQosFilter(
          binder,
          "/druid/worker/v1/chat/*",
          maxRequests
      );
    }

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
        config,
        TLSServerConfig,
        injector.getExistingBinding(Key.get(SslContextFactory.Server.class)),
        injector.getInstance(TLSCertificateChecker.class)
    );
  }
}
