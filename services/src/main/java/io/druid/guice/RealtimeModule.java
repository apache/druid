/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.druid.cli.QueryJettyServerInitializer;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.realtime.DbSegmentPublisher;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.NoopSegmentPublisher;
import io.druid.segment.realtime.RealtimeManager;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.ChatHandlerResource;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import io.druid.server.QueryResource;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
public class RealtimeModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        "druid.publish.type",
        Key.get(SegmentPublisher.class),
        Key.get(DbSegmentPublisher.class)
    );
    final MapBinder<String, SegmentPublisher> publisherBinder = PolyBind.optionBinder(
        binder,
        Key.get(SegmentPublisher.class)
    );
    publisherBinder.addBinding("noop").to(NoopSegmentPublisher.class);
    binder.bind(DbSegmentPublisher.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        "druid.realtime.chathandler.type",
        Key.get(ChatHandlerProvider.class),
        Key.get(NoopChatHandlerProvider.class)
    );
    final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
        binder, Key.get(ChatHandlerProvider.class)
    );
    handlerProviderBinder.addBinding("announce")
                         .to(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
    handlerProviderBinder.addBinding("noop")
                         .to(NoopChatHandlerProvider.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);
    binder.bind(new TypeLiteral<List<FireDepartment>>(){})
          .toProvider(FireDepartmentsProvider.class)
          .in(LazySingleton.class);

    binder.bind(QuerySegmentWalker.class).to(RealtimeManager.class).in(ManageLifecycle.class);
    binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("realtime"));
    binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
    Jerseys.addResource(binder, QueryResource.class);
    Jerseys.addResource(binder, ChatHandlerResource.class);
    LifecycleModule.register(binder, QueryResource.class);

    LifecycleModule.register(binder, Server.class);
  }
}
