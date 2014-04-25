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

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;
import io.tesla.aether.internal.DefaultTeslaAether;

import java.util.List;


@Command(
    name = "pull-deps",
    description = "Pull down dependencies to the local repository specified by druid.extensions.localRepository"
)
public class PullDependencies implements Runnable
{
  @Option(name = "-c",
          title = "coordinate",
          description = "extra dependencies to pull down (e.g. hadoop coordinates)",
          required = false)
  public List<String> coordinates;

  @Option(name = "--no-default-hadoop",
          description = "don't pull down the default HadoopIndexTask dependencies",
          required = false)
  public boolean noDefaultHadoop;

  @Inject
  public ExtensionsConfig extensionsConfig = null;

  @Override
  public void run()
  {
    // Druid dependencies are pulled down as a side-effect of Guice injection. Extra dependencies are pulled down as
    // a side-effect of getting class loaders.
    final List<String> allCoordinates = Lists.newArrayList();
    if (coordinates != null) {
      allCoordinates.addAll(coordinates);
    }
    if (!noDefaultHadoop) {
      allCoordinates.add(HadoopIndexTask.DEFAULT_HADOOP_COORDINATES);
    }
    try {
      final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);
      for (final String coordinate : allCoordinates) {
        Initialization.getClassLoaderForCoordinates(aetherClient, coordinate);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
