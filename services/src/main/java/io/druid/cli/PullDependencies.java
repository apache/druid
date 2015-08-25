/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.ExtensionsConfig;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.initialization.Initialization;
import io.tesla.aether.internal.DefaultTeslaAether;

import java.util.List;


@Command(
    name = "pull-deps",
    description = "Pull down dependencies to the local repository specified by druid.extensions.localRepository"
)
public class PullDependencies implements Runnable
{
  @Option(name = {"-c", "--coordinate"},
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
      allCoordinates.addAll(TaskConfig.DEFAULT_DEFAULT_HADOOP_COORDINATES);
    }
    try {
      final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);
      for (final String coordinate : allCoordinates) {
        Initialization.getClassLoaderForCoordinates(aetherClient, coordinate, extensionsConfig.getDefaultVersion());
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
