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

package org.apache.druid.grpc;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ManageLifecycleLast;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.DruidNode;

import javax.inject.Singleton;
import java.util.List;

public class GrpcQueryModule implements DruidModule
{
  private static final String GRPC_PREFIX = "org.apache.druid.grpc";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, GRPC_PREFIX, GrpcConfig.class);
    binder.bind(QueryServer.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, QueryServer.class);
    Multibinder.newSetBinder(binder, DruidNode.class)
               .addBinding()
               .to(Key.get(DruidNode.class, Grpc.class));
    binder.bind(DruidLastAnnouncer.class).in(ManageLifecycleLast.class);
    LifecycleModule.register(binder, DruidLastAnnouncer.class);
  }

  @Provides
  @Grpc
  @Singleton
  public DruidNode grpcDruidNode(
      GrpcConfig config,
      @Self DruidNode self
  )
  {
    return new DruidNode(
        config.getServiceName(),
        self.getHost(),
        true,
        config.getPort(),
        null,
        true,
        false
    );
  }
}
