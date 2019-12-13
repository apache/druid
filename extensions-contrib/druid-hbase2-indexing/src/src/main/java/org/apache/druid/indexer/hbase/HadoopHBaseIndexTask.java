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

package org.apache.druid.indexer.hbase;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HadoopHBaseIndexTask extends HadoopIndexTask
{
  private static final Logger LOG = LogManager.getLogger(HadoopHBaseIndexTask.class);

  private static final ExtensionsConfig EXTENSIONS_CONFIG;

  static final Injector INJECTOR = GuiceInjectors.makeStartupInjector();

  static {
    EXTENSIONS_CONFIG = INJECTOR.getInstance(ExtensionsConfig.class);
  }

  public HadoopHBaseIndexTask(@JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper, @JsonProperty("context") Map<String, Object> context,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider)
  {
    super(id, spec, hadoopCoordinates, hadoopDependencyCoordinates, classpathPrefix, jsonMapper,
        context, authorizerMapper, chatHandlerProvider);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.druid.indexing.common.task.Task#getType()
   */
  @Override
  public String getType()
  {
    return "index_hadoop_hbase";
  }

  @Override
  protected ClassLoader buildClassLoader(final TaskToolbox toolbox)
  {
    return buildClassLoaderForHBase(getHadoopDependencyCoordinates(),
        toolbox.getConfig().getDefaultHadoopCoordinates());
  }

  private ClassLoader buildClassLoaderForHBase(final List<String> hadoopDependencyCoordinates,
      final List<String> defaultHadoopCoordinates)
  {
    final List<String> finalHadoopDependencyCoordinates = hadoopDependencyCoordinates != null
        ? hadoopDependencyCoordinates
        : defaultHadoopCoordinates;

    // Exclude if there is a version other than 2.5 of protobuf-java for HBase.
    final List<URL> jobURLs = Arrays
        .stream(((URLClassLoader) HadoopIndexTask.class.getClassLoader()).getURLs()).filter(url -> {
          String[] tokens = url.getFile().split(File.separator);
          String file = tokens[tokens.length - 1];
          return !file.startsWith("protobuf-java") || file.endsWith("2.5.0.jar");
        }).collect(Collectors.toList());

    if (LOG.isDebugEnabled()) {
      LOG.debug("urls for current class-loader: {}", jobURLs);
    }

    final List<URL> extensionURLs = new ArrayList<>();
    for (final File extension : Initialization.getExtensionFilesToLoad(EXTENSIONS_CONFIG)) {
      final ClassLoader extensionLoader = Initialization.getClassLoaderForExtension(extension,
          false);
      extensionURLs.addAll(Arrays.asList(((URLClassLoader) extensionLoader).getURLs()));
    }

    jobURLs.addAll(extensionURLs);

    final List<URL> localClassLoaderURLs = new ArrayList<>(jobURLs);

    // hadoop dependencies come before druid classes because some extensions
    // depend on them
    for (final File hadoopDependency : Initialization
        .getHadoopDependencyFilesToLoad(finalHadoopDependencyCoordinates, EXTENSIONS_CONFIG)) {
      final ClassLoader hadoopLoader = Initialization.getClassLoaderForExtension(hadoopDependency,
          false);
      localClassLoaderURLs.addAll(Arrays.asList(((URLClassLoader) hadoopLoader).getURLs()));
    }

    final ClassLoader classLoader = new URLClassLoader(localClassLoaderURLs.toArray(new URL[0]),
        null);

    final String hadoopContainerDruidClasspathJars;
    if (EXTENSIONS_CONFIG.getHadoopContainerDruidClasspath() == null) {
      hadoopContainerDruidClasspathJars = Joiner.on(File.pathSeparator).join(jobURLs);

    } else {
      List<URL> hadoopContainerURLs = Lists.newArrayList(
          Initialization.getURLsForClasspath(EXTENSIONS_CONFIG.getHadoopContainerDruidClasspath()));

      if (EXTENSIONS_CONFIG.getAddExtensionsToHadoopContainer()) {
        hadoopContainerURLs.addAll(extensionURLs);
      }

      hadoopContainerDruidClasspathJars = Joiner.on(File.pathSeparator).join(hadoopContainerURLs);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Hadoop Container Druid Classpath is set to [{}]",
          hadoopContainerDruidClasspathJars);
    }
    System.setProperty("druid.hadoop.internal.classpath", hadoopContainerDruidClasspathJars);

    return classLoader;
  }

}
