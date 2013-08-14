/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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
import com.metamx.druid.guice.DruidProcessingModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.QueryRunnerFactoryModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.StorageNodeModule;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.realtime.RealtimeManager;
import druid.examples.guice.RealtimeStandaloneModule;
import io.airlift.command.Command;

/**
 */
@Command(
    name = "realtimeStandalone",
    description = "Runs a standalone realtime node for examples, see https://github.com/metamx/druid/wiki/Realtime for a description"
)
public class CliRealtimeStandalone extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliRealtimeStandalone()
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
        StorageNodeModule.class,
        new JettyServerModule(new QueryJettyServerInitializer())
            .addResource(StatusResource.class),
        new QueryableModule(RealtimeManager.class),
        new QueryRunnerFactoryModule(),
        RealtimeStandaloneModule.class
    );
  }
}
