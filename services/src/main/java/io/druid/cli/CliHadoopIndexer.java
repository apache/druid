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
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;
import io.tesla.aether.internal.DefaultTeslaAether;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

/**
 */
@Command(
    name = "hadoop",
    description = "Runs the batch Hadoop Druid Indexer, see http://druid.io/docs/0.6.10/Batch-ingestion.html for a description."
)
public class CliHadoopIndexer implements Runnable
{
  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  @Option(name = "hadoop", description = "The maven coordinates to the version of hadoop to run with. Defaults to org.apache.hadoop:hadoop-core:1.0.3")
  private String hadoopCoordinates = "org.apache.hadoop:hadoop-core:1.0.3";

  @Inject
  private ExtensionsConfig extensionsConfig = null;

  @Override
  @SuppressWarnings("unchecked")
  public void run()
  {
    try {
      final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);
      final ClassLoader hadoopLoader = Initialization.getClassLoaderForCoordinates(
          aetherClient, hadoopCoordinates
      );
      final URL[] urLs = ((URLClassLoader) hadoopLoader).getURLs();

      final URL[] nonHadoopUrls = ((URLClassLoader) CliHadoopIndexer.class.getClassLoader()).getURLs();

      List<URL> theURLS = Lists.newArrayList();
      theURLS.addAll(Arrays.asList(urLs));
      theURLS.addAll(Arrays.asList(nonHadoopUrls));

      final URLClassLoader loader = new URLClassLoader(theURLS.toArray(new URL[theURLS.size()]), null);
      Thread.currentThread().setContextClassLoader(loader);

      System.setProperty("druid.hadoop.internal.classpath", Joiner.on(File.pathSeparator).join(nonHadoopUrls));

      final Class<?> mainClass = loader.loadClass(Main.class.getName());
      final Method mainMethod = mainClass.getMethod("main", String[].class);

      String[] args = new String[]{
          "internal",
          "hadoop-indexer",
          argumentSpec
      };

      mainMethod.invoke(null, new Object[]{args});
    }
    catch (Exception e) {
      log.error(e, "failure!!!!");
      System.exit(1);
    }
  }

}
