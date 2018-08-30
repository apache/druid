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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.cli.QueryJettyServerInitializer;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.metadata.MetadataSegmentPublisher;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.NoopSegmentPublisher;
import org.apache.druid.segment.realtime.RealtimeManager;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierConfig;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierFactory;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.ZkCoordinator;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
public class RealtimeModule implements Module
{

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoiceWithDefault(binder, "druid.publish.type", Key.get(SegmentPublisher.class), "metadata");
    final MapBinder<String, SegmentPublisher> publisherBinder = PolyBind.optionBinder(
        binder,
        Key.get(SegmentPublisher.class)
    );
    publisherBinder.addBinding("noop").to(NoopSegmentPublisher.class).in(LazySingleton.class);
    publisherBinder.addBinding("metadata").to(MetadataSegmentPublisher.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        "druid.realtime.rowIngestionMeters.type",
        Key.get(RowIngestionMetersFactory.class),
        Key.get(DropwizardRowIngestionMetersFactory.class)
    );
    final MapBinder<String, RowIngestionMetersFactory> rowIngestionMetersHandlerProviderBinder = PolyBind.optionBinder(
        binder, Key.get(RowIngestionMetersFactory.class)
    );
    rowIngestionMetersHandlerProviderBinder.addBinding("dropwizard")
                                           .to(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);
    binder.bind(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        "druid.realtime.chathandler.type",
        Key.get(ChatHandlerProvider.class),
        Key.get(ServiceAnnouncingChatHandlerProvider.class)
    );
    final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
        binder, Key.get(ChatHandlerProvider.class)
    );
    handlerProviderBinder.addBinding("announce")
                         .to(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
    handlerProviderBinder.addBinding("noop")
                         .to(NoopChatHandlerProvider.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);
    binder.bind(
        new TypeLiteral<List<FireDepartment>>()
        {
        }
    )
          .toProvider(FireDepartmentsProvider.class)
          .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.segment.handoff", CoordinatorBasedSegmentHandoffNotifierConfig.class);
    binder.bind(SegmentHandoffNotifierFactory.class)
          .to(CoordinatorBasedSegmentHandoffNotifierFactory.class)
          .in(LazySingleton.class);
    binder.bind(CoordinatorClient.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.realtime.cache", CacheConfig.class);
    binder.install(new CacheModule());

    binder.bind(QuerySegmentWalker.class).to(RealtimeManager.class).in(ManageLifecycle.class);
    binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(ServerType.REALTIME));
    binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
    binder.bind(QueryCountStatsProvider.class).to(QueryResource.class).in(LazySingleton.class);
    Jerseys.addResource(binder, QueryResource.class);
    Jerseys.addResource(binder, SegmentListerResource.class);
    LifecycleModule.register(binder, QueryResource.class);
    LifecycleModule.register(binder, Server.class);

    binder.bind(SegmentManager.class).in(LazySingleton.class);
    binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, ZkCoordinator.class);
  }
}
