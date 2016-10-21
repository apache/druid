/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.ExtensionsConfig;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.logger.Logger;

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

  private static final List<String> DEFAULT_HADOOP_COORDINATES = TaskConfig.DEFAULT_DEFAULT_HADOOP_COORDINATES;

  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object", required = true)
  private String argumentSpec;

  @Option(name = {"-c", "--coordinate", "hadoopDependencies"},
          description = "extra dependencies to pull down (e.g. non-default hadoop coordinates or extra hadoop jars)")
  private List<String> coordinates;

  @Option(name = "--no-default-hadoop",
          description = "don't pull down the default hadoop version",
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
        allCoordinates.addAll(DEFAULT_HADOOP_COORDINATES);
      }

      final List<URL> extensionURLs = Lists.newArrayList();
      for (final File extension : Initialization.getExtensionFilesToLoad(extensionsConfig)) {
        final ClassLoader extensionLoader = Initialization.getClassLoaderForExtension(extension);
        extensionURLs.addAll(Arrays.asList(((URLClassLoader) extensionLoader).getURLs()));
      }

      final List<URL> nonHadoopURLs = Lists.newArrayList();
      nonHadoopURLs.addAll(Arrays.asList(((URLClassLoader) CliHadoopIndexer.class.getClassLoader()).getURLs()));

      final List<URL> driverURLs = Lists.newArrayList();
      driverURLs.addAll(nonHadoopURLs);
      // put hadoop dependencies last to avoid jets3t & apache.httpcore version conflicts
      for (File hadoopDependency : Initialization.getHadoopDependencyFilesToLoad(allCoordinates, extensionsConfig)) {
        final ClassLoader hadoopLoader = Initialization.getClassLoaderForExtension(hadoopDependency);
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
