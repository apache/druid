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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.ExtensionsConfig;
import io.druid.initialization.Initialization;
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
          description = "extra dependencies to pull down (e.g. non-default hadoop coordinates or extra hadoop jars)")
  private List<String> coordinates;

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
        allCoordinates.addAll(coordinates);
      }
      if (!noDefaultHadoop) {
        allCoordinates.add(DEFAULT_HADOOP_COORDINATES);
      }

      final DefaultTeslaAether aetherClient = Initialization.getAetherClient(extensionsConfig);

      final List<URL> extensionURLs = Lists.newArrayList();
      for (String coordinate : extensionsConfig.getCoordinates()) {
        final ClassLoader coordinateLoader = Initialization.getClassLoaderForCoordinates(
            aetherClient, coordinate, extensionsConfig.getDefaultVersion()
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
            aetherClient, coordinate, extensionsConfig.getDefaultVersion()
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
