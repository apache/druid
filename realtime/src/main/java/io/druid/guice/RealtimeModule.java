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

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.metamx.common.logger.Logger;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.RealtimeManager;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.KafkaFirehoseFactory;

import java.util.Arrays;
import java.util.List;

/**
 */
public class RealtimeModule implements DruidModule
{
  private static final Logger log = new Logger(RealtimeModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.publish", SegmentPublisherProvider.class);
    binder.bind(SegmentPublisher.class).toProvider(SegmentPublisherProvider.class);

    JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);
    binder.bind(
        new TypeLiteral<List<FireDepartment>>()
        {
        }
    ).toProvider(FireDepartmentsProvider.class).in(LazySingleton.class);
    binder.bind(RealtimeManager.class).in(ManageLifecycle.class);
  }

  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return Arrays.<com.fasterxml.jackson.databind.Module>asList(
        new SimpleModule("RealtimeModule")
            .registerSubtypes(
                new NamedType(KafkaFirehoseFactory.class, "kafka-0.7.2")
            )
    );
  }
}
