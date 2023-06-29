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

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import org.apache.druid.guice.ExtensionsLoader;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Initialization;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
@Command(
    name = "hadoop",
    description = "Runs the batch Hadoop Druid Indexer, see https://druid.apache.org/docs/latest/Batch-ingestion.html for a description."
)
public class CliHadoopIndexer implements Runnable
{
  private static final List<String> DEFAULT_HADOOP_COORDINATES = TaskConfig.DEFAULT_DEFAULT_HADOOP_COORDINATES;

  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object")
  @Required
  private String argumentSpec;

  @Option(name = {"-c", "--coordinate", "hadoopDependencies"},
          description = "extra dependencies to pull down (e.g. non-default hadoop coordinates or extra hadoop jars)")
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private List<String> coordinates;

  @Option(name = "--no-default-hadoop",
          description = "don't pull down the default hadoop version")
  public boolean noDefaultHadoop;

  @Inject
  private ExtensionsLoader extnLoader;

  @Override
  public void run()
  {
    try {
      final List<String> allCoordinates = new ArrayList<>();
      if (coordinates != null) {
        allCoordinates.addAll(coordinates);
      }
      if (!noDefaultHadoop) {
        allCoordinates.addAll(DEFAULT_HADOOP_COORDINATES);
      }

      final List<URL> extensionURLs = new ArrayList<>();
      for (final File extension : extnLoader.getExtensionFilesToLoad()) {
        final URLClassLoader extensionLoader = extnLoader.getClassLoaderForExtension(extension, false);
        extensionURLs.addAll(Arrays.asList(extensionLoader.getURLs()));
      }

      final List<URL> nonHadoopURLs = Arrays.asList(
          ((URLClassLoader) CliHadoopIndexer.class.getClassLoader()).getURLs()
      );

      final List<URL> driverURLs = new ArrayList<>(nonHadoopURLs);
      // put hadoop dependencies last to avoid jets3t & apache.httpcore version conflicts
      for (File hadoopDependency : Initialization.getHadoopDependencyFilesToLoad(allCoordinates, extnLoader.config())) {
        final URLClassLoader hadoopLoader = extnLoader.getClassLoaderForExtension(hadoopDependency, false);
        driverURLs.addAll(Arrays.asList(hadoopLoader.getURLs()));
      }

      final URLClassLoader loader = new URLClassLoader(driverURLs.toArray(new URL[0]), null);
      Thread.currentThread().setContextClassLoader(loader);

      final List<URL> jobUrls = new ArrayList<>();
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
