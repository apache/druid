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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.initialization.Initialization;
import io.druid.guice.ExtensionsConfig;
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
    description = "Runs the batch Hadoop Druid Indexer, see http://druid.io/docs/latest/Batch-ingestion.html for a description."
)
public class CliHadoopIndexer implements Runnable
{

  private static final String DEFAULT_HADOOP_COORDINATES = "org.apache.hadoop:hadoop-client:2.3.0";

  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  @Option(name = {"-c", "--coordinate", "hadoopDependencies"},
          description = "comma separated list of extra dependencies to pull down (e.g. non-default hadoop coordinates or extra hadoop jars)")
  private String coordinates;

  @Option(name = "--no-default-hadoop",
          description = "don't pull down the default hadoop version (currently " + DEFAULT_HADOOP_COORDINATES + ")",
          required = false)
  public boolean noDefaultHadoop;

  @Inject
  private ExtensionsConfig extensionsConfig = null;

  @Override
  @SuppressWarnings("unchecked")
  public void run()
  {
    try {
      final List<String> allCoordinates = Lists.newArrayList();
      if (coordinates != null) {
        allCoordinates.addAll(Arrays.asList(coordinates.replaceAll("\\s+", "").split(",")));
      }
      if (!noDefaultHadoop) {
        allCoordinates.add(DEFAULT_HADOOP_COORDINATES);
      }

      final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);

      final List<URL> extensionURLs = Lists.newArrayList();
      for (String coordinate : extensionsConfig.getCoordinates()) {
        final ClassLoader coordinateLoader = Initialization.getClassLoaderForCoordinates(
            aetherClient, coordinate
        );
        extensionURLs.addAll(Arrays.asList(((URLClassLoader) coordinateLoader).getURLs()));
      }

      final List<URL> nonHadoopURLs = Lists.newArrayList();
      nonHadoopURLs.addAll(Arrays.asList(((URLClassLoader) CliHadoopIndexer.class.getClassLoader()).getURLs()));

      final List<URL> driverURLs = Lists.newArrayList();
      driverURLs.addAll(nonHadoopURLs);
      // put hadoop dependencies last to avoid jets3t & apache.httpcore version conflicts
      for (String coordinate : allCoordinates) {
        final ClassLoader hadoopLoader = Initialization.getClassLoaderForCoordinates(
            aetherClient, coordinate
        );
        driverURLs.addAll(Arrays.asList(((URLClassLoader) hadoopLoader).getURLs()));
      }

      final URLClassLoader loader = new URLClassLoader(driverURLs.toArray(new URL[driverURLs.size()]), null);
      Thread.currentThread().setContextClassLoader(loader);

      final List<URL> jobUrls = Lists.newArrayList();
      jobUrls.addAll(nonHadoopURLs);
      jobUrls.addAll(extensionURLs);

      System.setProperty("druid.hadoop.internal.classpath", Joiner.on(File.pathSeparator).join(jobUrls));

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
