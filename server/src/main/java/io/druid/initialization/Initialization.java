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

package io.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.CoordinatorDiscoveryModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.FirehoseModule;
import io.druid.guice.IndexingServiceDiscoveryModule;
import io.druid.guice.JacksonConfigManagerModule;
import io.druid.guice.JavaScriptModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.guice.MetadataConfigModule;
import io.druid.guice.ParsersModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StartupLoggingModule;
import io.druid.guice.StorageNodeModule;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.HttpClientModule;
import io.druid.guice.security.DruidAuthModule;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.jetty.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import org.apache.commons.io.FileUtils;
import org.eclipse.aether.artifact.DefaultArtifact;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);
  private static final ConcurrentMap<File, URLClassLoader> loadersMap = new ConcurrentHashMap<>();

  private final static Map<Class, Set> extensionsMap = Maps.<Class, Set>newHashMap();

  /**
   * @param clazz Module class
   * @param <T>
   *
   * @return Returns the set of modules loaded.
   */
  public static <T> Set<T> getLoadedModules(Class<T> clazz)
  {
    Set<T> retVal = extensionsMap.get(clazz);
    if (retVal == null) {
      return Sets.newHashSet();
    }
    return retVal;
  }

  @VisibleForTesting
  static void clearLoadedModules()
  {
    extensionsMap.clear();
  }

  @VisibleForTesting
  static Map<File, URLClassLoader> getLoadersMap()
  {
    return loadersMap;
  }

  /**
   * Look for extension modules for the given class from both classpath and extensions directory. A user should never
   * put the same two extensions in classpath and extensions directory, if he/she does that, the one that is in the
   * classpath will be loaded, the other will be ignored.
   *
   * @param config Extensions configuration
   * @param clazz  The class of extension module (e.g., DruidModule)
   *
   * @return A collection that contains distinct extension modules
   */
  public synchronized static <T> Collection<T> getFromExtensions(ExtensionsConfig config, Class<T> clazz)
  {
    final Set<T> retVal = Sets.newHashSet();
    final Set<String> loadedExtensionNames = Sets.newHashSet();

    if (config.searchCurrentClassloader()) {
      for (T module : ServiceLoader.load(clazz, Thread.currentThread().getContextClassLoader())) {
        final String moduleName = module.getClass().getCanonicalName();
        if (moduleName == null) {
          log.warn(
              "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
              module.getClass().getName()
          );
        } else if (!loadedExtensionNames.contains(moduleName)) {
          log.info("Adding classpath extension module [%s] for class [%s]", moduleName, clazz.getName());
          loadedExtensionNames.add(moduleName);
          retVal.add(module);
        }
      }
    }

    for (File extension : getExtensionFilesToLoad(config)) {
      log.info("Loading extension [%s] for class [%s]", extension.getName(), clazz.getName());
      try {
        final URLClassLoader loader = getClassLoaderForExtension(extension);
        for (T module : ServiceLoader.load(clazz, loader)) {
          final String moduleName = module.getClass().getCanonicalName();
          if (moduleName == null) {
            log.warn(
                "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
                module.getClass().getName()
            );
          } else if (!loadedExtensionNames.contains(moduleName)) {
            log.info("Adding local file system extension module [%s] for class [%s]", moduleName, clazz.getName());
            loadedExtensionNames.add(moduleName);
            retVal.add(module);
          }
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    // update the map with currently loaded modules
    extensionsMap.put(clazz, retVal);

    return retVal;
  }

  /**
   * Find all the extension files that should be loaded by druid.
   * <p/>
   * If user explicitly specifies druid.extensions.loadList, then it will look for those extensions under root
   * extensions directory. If one of them is not found, druid will fail loudly.
   * <p/>
   * If user doesn't specify druid.extension.toLoad (or its value is empty), druid will load all the extensions
   * under the root extensions directory.
   *
   * @param config ExtensionsConfig configured by druid.extensions.xxx
   *
   * @return an array of druid extension files that will be loaded by druid process
   */
  public static File[] getExtensionFilesToLoad(ExtensionsConfig config)
  {
    final File rootExtensionsDir = new File(config.getDirectory());
    if (rootExtensionsDir.exists() && !rootExtensionsDir.isDirectory()) {
      throw new ISE("Root extensions directory [%s] is not a directory!?", rootExtensionsDir);
    }
    File[] extensionsToLoad;
    final List<String> toLoad = config.getLoadList();
    if (toLoad == null) {
      extensionsToLoad = rootExtensionsDir.listFiles();
    } else {
      int i = 0;
      extensionsToLoad = new File[toLoad.size()];
      for (final String extensionName : toLoad) {
        final File extensionDir = new File(rootExtensionsDir, extensionName);
        if (!extensionDir.isDirectory()) {
          throw new ISE(
              String.format(
                  "Extension [%s] specified in \"druid.extensions.loadList\" didn't exist!?",
                  extensionDir.getAbsolutePath()
              )
          );
        }
        extensionsToLoad[i++] = extensionDir;
      }
    }
    return extensionsToLoad == null ? new File[]{} : extensionsToLoad;
  }

  /**
   * Find all the hadoop dependencies that should be loaded by druid
   *
   * @param hadoopDependencyCoordinates e.g.["org.apache.hadoop:hadoop-client:2.3.0"]
   * @param extensionsConfig            ExtensionsConfig configured by druid.extensions.xxx
   *
   * @return an array of hadoop dependency files that will be loaded by druid process
   */
  public static File[] getHadoopDependencyFilesToLoad(
      List<String> hadoopDependencyCoordinates,
      ExtensionsConfig extensionsConfig
  )
  {
    final File rootHadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());
    if (rootHadoopDependenciesDir.exists() && !rootHadoopDependenciesDir.isDirectory()) {
      throw new ISE("Root Hadoop dependencies directory [%s] is not a directory!?", rootHadoopDependenciesDir);
    }
    final File[] hadoopDependenciesToLoad = new File[hadoopDependencyCoordinates.size()];
    int i = 0;
    for (final String coordinate : hadoopDependencyCoordinates) {
      final DefaultArtifact artifact = new DefaultArtifact(coordinate);
      final File hadoopDependencyDir = new File(rootHadoopDependenciesDir, artifact.getArtifactId());
      final File versionDir = new File(hadoopDependencyDir, artifact.getVersion());
      // find the hadoop dependency with the version specified in coordinate
      if (!hadoopDependencyDir.isDirectory() || !versionDir.isDirectory()) {
        throw new ISE(
            String.format("Hadoop dependency [%s] didn't exist!?", versionDir.getAbsolutePath())
        );
      }
      hadoopDependenciesToLoad[i++] = versionDir;
    }
    return hadoopDependenciesToLoad;
  }

  /**
   * @param extension The File instance of the extension we want to load
   *
   * @return a URLClassLoader that loads all the jars on which the extension is dependent
   *
   * @throws MalformedURLException
   */
  public static URLClassLoader getClassLoaderForExtension(File extension) throws MalformedURLException
  {
    URLClassLoader loader = loadersMap.get(extension);
    if (loader == null) {
      final Collection<File> jars = FileUtils.listFiles(extension, new String[]{"jar"}, false);
      final URL[] urls = new URL[jars.size()];
      int i = 0;
      for (File jar : jars) {
        final URL url = jar.toURI().toURL();
        log.info("added URL[%s]", url);
        urls[i++] = url;
      }
      loadersMap.putIfAbsent(extension, new URLClassLoader(urls, Initialization.class.getClassLoader()));
      loader = loadersMap.get(extension);
    }
    return loader;
  }

  public static List<URL> getURLsForClasspath(String cp)
  {
    try {
      String[] paths = cp.split(File.pathSeparator);

      List<URL> urls = new ArrayList<>();
      for (int i = 0; i < paths.length; i++) {
        File f = new File(paths[i]);
        if ("*".equals(f.getName())) {
          File parentDir = f.getParentFile();
          if (parentDir.isDirectory()) {
            File[] jars = parentDir.listFiles(
                new FilenameFilter()
                {
                  @Override
                  public boolean accept(File dir, String name)
                  {
                    return name != null && (name.endsWith(".jar") || name.endsWith(".JAR"));
                  }
                }
            );
            for (File jar : jars) {
              urls.add(jar.toURI().toURL());
            }
          }
        } else {
          urls.add(new File(paths[i]).toURI().toURL());
        }
      }
      return urls;
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static Injector makeInjectorWithModules(final Injector baseInjector, Iterable<? extends Module> modules)
  {
    final ModuleList defaultModules = new ModuleList(baseInjector);
    defaultModules.addModules(
        // New modules should be added after Log4jShutterDownerModule
        new Log4jShutterDownerModule(),
        new DruidAuthModule(),
        new LifecycleModule(),
        EmitterModule.class,
        HttpClientModule.global(),
        new HttpClientModule("druid.broker.http", Client.class),
        new CuratorModule(),
        new AnnouncerModule(),
        new DruidProcessingModule(),
        new AWSModule(),
        new MetricsModule(),
        new ServerModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageDruidModule(),
        new JacksonConfigManagerModule(),
        new IndexingServiceDiscoveryModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageDruidModule(),
        new FirehoseModule(),
        new ParsersModule(),
        new JavaScriptModule(),
        new StartupLoggingModule()
    );

    ModuleList actualModules = new ModuleList(baseInjector);
    actualModules.addModule(DruidSecondaryModule.class);
    for (Object module : modules) {
      actualModules.addModule(module);
    }

    Module intermediateModules = Modules.override(defaultModules.getModules()).with(actualModules.getModules());

    ModuleList extensionModules = new ModuleList(baseInjector);
    final ExtensionsConfig config = baseInjector.getInstance(ExtensionsConfig.class);
    for (DruidModule module : Initialization.getFromExtensions(config, DruidModule.class)) {
      extensionModules.addModule(module);
    }

    return Guice.createInjector(Modules.override(intermediateModules).with(extensionModules.getModules()));
  }

  private static class ModuleList
  {
    private final Injector baseInjector;
    private final ObjectMapper jsonMapper;
    private final ObjectMapper smileMapper;
    private final List<Module> modules;

    public ModuleList(Injector baseInjector)
    {
      this.baseInjector = baseInjector;
      this.jsonMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
      this.smileMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Smile.class));
      this.modules = Lists.newArrayList();
    }

    private List<Module> getModules()
    {
      return Collections.unmodifiableList(modules);
    }

    public void addModule(Object input)
    {
      if (input instanceof DruidModule) {
        baseInjector.injectMembers(input);
        modules.add(registerJacksonModules(((DruidModule) input)));
      } else if (input instanceof Module) {
        baseInjector.injectMembers(input);
        modules.add((Module) input);
      } else if (input instanceof Class) {
        if (DruidModule.class.isAssignableFrom((Class) input)) {
          modules.add(registerJacksonModules(baseInjector.getInstance((Class<? extends DruidModule>) input)));
        } else if (Module.class.isAssignableFrom((Class) input)) {
          modules.add(baseInjector.getInstance((Class<? extends Module>) input));
          return;
        } else {
          throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
        }
      } else {
        throw new ISE("Unknown module type[%s]", input.getClass());
      }
    }

    public void addModules(Object... object)
    {
      for (Object o : object) {
        addModule(o);
      }
    }

    private DruidModule registerJacksonModules(DruidModule module)
    {
      for (com.fasterxml.jackson.databind.Module jacksonModule : module.getJacksonModules()) {
        jsonMapper.registerModule(jacksonModule);
        smileMapper.registerModule(jacksonModule);
      }
      return module;
    }
  }
}
