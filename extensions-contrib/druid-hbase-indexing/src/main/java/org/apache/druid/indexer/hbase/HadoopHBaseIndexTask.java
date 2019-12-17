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
import com.google.inject.Injector;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.hbase.util.HBaseUtil;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.JvmUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
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

    ClassLoader taskClassLoader = HadoopIndexTask.class.getClassLoader();
    final List<URL> jobURLs;
    // Remove the library that is causing the crash.
    Map<String, String> removingLibMap = getRemovingLibraries();
    if (taskClassLoader instanceof URLClassLoader) {
      // this only works with Java 8
      jobURLs = HBaseUtil.removeConflictingLibrary(Arrays.stream(((URLClassLoader) taskClassLoader).getURLs()),
          removingLibMap);
    } else {
      // fallback to parsing system classpath
      jobURLs = HBaseUtil.removeConflictingLibrary(JvmUtils.systemClassPath().stream(), removingLibMap);
    }

    final List<URL> extensionURLs = new ArrayList<>();
    for (final File extension : Initialization.getExtensionFilesToLoad(EXTENSIONS_CONFIG)) {
      final URLClassLoader extensionLoader = Initialization.getClassLoaderForExtension(extension, false);
      extensionURLs
          .addAll(HBaseUtil.removeConflictingLibrary(Arrays.stream(extensionLoader.getURLs()), removingLibMap));
    }

    jobURLs.addAll(extensionURLs);

    final List<URL> localClassLoaderURLs = new ArrayList<>(jobURLs);

    // hadoop dependencies come before druid classes because some extensions
    // depend on them
    for (final File hadoopDependency : Initialization.getHadoopDependencyFilesToLoad(
        finalHadoopDependencyCoordinates,
        EXTENSIONS_CONFIG)) {
      final URLClassLoader hadoopLoader = Initialization.getClassLoaderForExtension(hadoopDependency, false);
      localClassLoaderURLs.addAll(Arrays.asList(hadoopLoader.getURLs()));
    }

    ClassLoader parent = null;
    if (JvmUtils.isIsJava9Compatible()) {
      try {
        // See also
        // https://docs.oracle.com/en/java/javase/11/migrate/index.html#JSMIG-GUID-A868D0B9-026F-4D46-B979-901834343F9E
        parent = (ClassLoader) ClassLoader.class.getMethod("getPlatformClassLoader").invoke(null);
      }
      catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }
    final ClassLoader classLoader = new URLClassLoader(
        localClassLoaderURLs.toArray(new URL[0]),
        parent);

    final String hadoopContainerDruidClasspathJars;
    if (EXTENSIONS_CONFIG.getHadoopContainerDruidClasspath() == null) {
      hadoopContainerDruidClasspathJars = Joiner.on(File.pathSeparator).join(jobURLs);
    } else {
      List<URL> hadoopContainerURLs = HBaseUtil.removeConflictingLibrary(
          Initialization.getURLsForClasspath(EXTENSIONS_CONFIG.getHadoopContainerDruidClasspath()).stream(),
          removingLibMap);

      if (EXTENSIONS_CONFIG.getAddExtensionsToHadoopContainer()) {
        hadoopContainerURLs.addAll(extensionURLs);
      }

      hadoopContainerDruidClasspathJars = Joiner.on(File.pathSeparator)
          .join(hadoopContainerURLs);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Hadoop Container Druid Classpath is set to {}", hadoopContainerDruidClasspathJars);
    }

    System.setProperty("druid.hadoop.internal.classpath", hadoopContainerDruidClasspathJars);

    return classLoader;
  }

  private Map<String, String> getRemovingLibraries()
  {
    Map<String, String> removingLibMap;
    InputStream is = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("druid-hbase-indexing.properties");

    try {
      if (is == null) {
        Path path = Paths.get("./extensions/druid-hbase-indexing/druid-hbase-indexing.properties");
        is = Files.newInputStream(path, StandardOpenOption.READ);
      }

      Properties props = new Properties();
      props.load(is);

      removingLibMap = Arrays
          .stream(props.getProperty("hadoop.remove.lib.in.classpath", "protobuf-java-3.,hbase-2.").split(","))
          .map(lib -> {
            Matcher m = HBaseUtil.LIBRARY_PATTERN.matcher(lib);
            if (m.find()) {
              final String artifact = m.group(1);
              final String version = m.group(2);

              return new Pair<String, String>(artifact, version);
            } else {
              return null;
            }
          })
          .filter(p -> {
            return p != null;
          })
          .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    }
    catch (IOException e) {
      removingLibMap = new HashMap<>();
      removingLibMap.put("hbase", "2.");
      removingLibMap.put("protobuf-java", "3.");
    }
    finally {
      if (is != null) {
        try {
          is.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("removing lib: {}", removingLibMap);
    }

    return removingLibMap;
  }
}
