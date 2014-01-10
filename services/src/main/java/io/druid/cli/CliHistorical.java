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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.NodeTypeConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.QueryResource;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "historical",
    description = "Runs a Historical node, see http://druid.io/docs/0.6.46/Historical.html for a description"
)
public class CliHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliHistorical.class);

  public CliHistorical()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(ServerManager.class).in(LazySingleton.class);
            binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
            binder.bind(QuerySegmentWalker.class).to(ServerManager.class).in(LazySingleton.class);

            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("historical"));
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            Jerseys.addResource(binder, QueryResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            LifecycleModule.register(binder, ZkCoordinator.class);
            LifecycleModule.register(binder, Server.class);
          }
        }
    );
  }
}
