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

package io.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.druid.guice.annotations.Self;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;

import java.util.Collections;
import java.util.List;

/**
 */
public class ServerModule implements DruidModule
{
  public static final String ZK_PATHS_PROPERTY_BASE = "druid.zk.paths";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, ZK_PATHS_PROPERTY_BASE, ZkPathsConfig.class);
    JsonConfigProvider.bind(binder, "druid", DruidNode.class, Self.class);
  }

  @Provides @LazySingleton
  public ScheduledExecutorFactory getScheduledExecutorFactory(Lifecycle lifecycle)
  {
    return ScheduledExecutors.createFactory(lifecycle);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
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
