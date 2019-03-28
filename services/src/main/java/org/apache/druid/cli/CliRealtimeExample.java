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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.InventoryView;
import org.apache.druid.client.ServerView;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.RealtimeModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.NoopDataSegmentPusher;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.server.initialization.jetty.ChatHandlerServerModule;
import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 */
@Command(
    name = "realtime",
    description = "Runs a standalone realtime node for examples, see http://druid.io/docs/latest/Realtime.html for a description"
)
public class CliRealtimeExample extends ServerRunnable
{
  private static final Logger log = new Logger(CliRealtimeExample.class);

  @Inject
  private Properties properties;

  public CliRealtimeExample()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new RealtimeModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/realtime");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8084);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8284);

          binder.bind(DataSegmentPusher.class).to(NoopDataSegmentPusher.class).in(LazySingleton.class);
          binder.bind(DataSegmentAnnouncer.class).to(NoopDataSegmentAnnouncer.class).in(LazySingleton.class);
          binder.bind(InventoryView.class).to(NoopInventoryView.class).in(LazySingleton.class);
          binder.bind(ServerView.class).to(NoopServerView.class).in(LazySingleton.class);
        },
        new ChatHandlerServerModule(properties),
        new LookupModule()
    );
  }

  private static class NoopServerView implements ServerView
  {
    @Override
    public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
    {
      // do nothing
    }

    @Override
    public void registerSegmentCallback(Executor exec, SegmentCallback callback)
    {
      // do nothing
    }
  }

  private static class NoopInventoryView implements InventoryView
  {
    @Override
    public DruidServer getInventoryValue(String serverKey)
    {
      return null;
    }

    @Override
    public Collection<DruidServer> getInventory()
    {
      return ImmutableList.of();
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      return false;
    }
  }
}
