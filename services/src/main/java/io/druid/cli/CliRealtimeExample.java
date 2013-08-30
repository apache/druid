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

package io.druid.cli;

import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import com.metamx.druid.realtime.RealtimeManager;
import druid.examples.guice.RealtimeExampleModule;
import io.airlift.command.Command;
import io.druid.guice.guice.DruidProcessingModule;
import io.druid.guice.guice.LifecycleModule;
import io.druid.guice.guice.QueryRunnerFactoryModule;
import io.druid.guice.guice.QueryableModule;
import io.druid.guice.guice.ServerModule;
import io.druid.guice.guice.StorageNodeModule;
import io.druid.server.StatusResource;
import io.druid.server.initialization.initialization.EmitterModule;
import io.druid.server.initialization.initialization.Initialization;
import io.druid.server.initialization.initialization.JettyServerModule;

/**
 */
@Command(
    name = "example realtime",
    description = "Runs a standalone realtime node for examples, see https://github.com/metamx/druid/wiki/Realtime for a description"
)
public class CliRealtimeExample extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliRealtimeExample()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule(),
        EmitterModule.class,
        DruidProcessingModule.class,
        new ServerModule(),
        new StorageNodeModule("realtime"),
        new JettyServerModule(new QueryJettyServerInitializer())
            .addResource(StatusResource.class),
        new QueryableModule(RealtimeManager.class),
        new QueryRunnerFactoryModule(),
        RealtimeExampleModule.class
    );
  }
}
