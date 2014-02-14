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
import com.google.inject.Provides;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.guice.annotations.Self;
import io.druid.initialization.DruidModule;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;

import java.util.Arrays;
import java.util.List;

/**
 */
public class ServerModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, ZkPathsConfig.class);

    JsonConfigProvider.bind(binder, "druid", DruidNode.class, Self.class);
  }

  @Provides @LazySingleton
  public ScheduledExecutorFactory getScheduledExecutorFactory(Lifecycle lifecycle)
  {
    return ScheduledExecutors.createFactory(lifecycle);
  }

  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return Arrays.asList(
        new SimpleModule()
            .registerSubtypes(
                new NamedType(SingleDimensionShardSpec.class, "single"),
                new NamedType(LinearShardSpec.class, "linear"),
                new NamedType(NumberedShardSpec.class, "numbered"),
                new NamedType(HashBasedNumberedShardSpec.class, "hashed")
            )
    );
  }
}
