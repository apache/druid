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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import io.druid.cli.QueryJettyServerInitializer;
import io.druid.initialization.DruidModule;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.RealtimeManager;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.KafkaFirehoseFactory;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.Arrays;
import java.util.List;

/**
*/
public class RealtimeModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.publish", SegmentPublisherProvider.class);
    binder.bind(SegmentPublisher.class).toProvider(SegmentPublisherProvider.class);

    JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);
    binder.bind(new TypeLiteral<List<FireDepartment>>(){})
          .toProvider(FireDepartmentsProvider.class)
          .in(LazySingleton.class);

    binder.bind(QuerySegmentWalker.class).to(RealtimeManager.class).in(ManageLifecycle.class);
    binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("realtime"));
    binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

    LifecycleModule.register(binder, Server.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("RealtimeModule")
            .registerSubtypes(
                new NamedType(KafkaFirehoseFactory.class, "kafka-0.7.2")
            )
    );
  }
}
