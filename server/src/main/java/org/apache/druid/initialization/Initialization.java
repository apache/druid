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

package org.apache.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.commons.io.FileUtils;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.CoordinatorDiscoveryModule;
import org.apache.druid.guice.DruidProcessingConfigModule;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.guice.IndexingServiceDiscoveryModule;
import org.apache.druid.guice.JacksonConfigManagerModule;
import org.apache.druid.guice.JavaScriptModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.ModulesConfig;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.ServerViewModule;
import org.apache.druid.guice.StartupLoggingModule;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.AuthenticatorModule;
import org.apache.druid.guice.security.AuthorizerModule;
import org.apache.druid.guice.security.DruidAuthModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.druid.server.emitter.EmitterModule;
import org.apache.druid.server.initialization.AuthenticatorMapperModule;
import org.apache.druid.server.initialization.AuthorizerMapperModule;
import org.apache.druid.server.initialization.jetty.JettyServerModule;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.server.security.TLSCertificateCheckerModule;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);
  private static final ConcurrentHashMap<File, URLClassLoader> LOADERS_MAP = new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<Class<?>, Collection<?>> EXTENSIONS_MAP = new ConcurrentHashMap<>();

  /**
   * @param clazz service class
   * @param <T>   the service type
   *
   * @return Returns a collection of implementations loaded.
   */
  public static <T> Collection<T> getLoadedImplementations(Class<T> clazz)
  {
    @SuppressWarnings("unchecked")
    Collection<T> retVal = (Collection<T>) EXTENSIONS_MAP.get(clazz);
    if (retVal == null) {
      return new HashSet<>();
    }
    return retVal;
  }

  @VisibleForTesting
  static void clearLoadedImplementations()
  {
    EXTENSIONS_MAP.clear();
  }

  @VisibleForTesting
  static Map<File, URLClassLoader> getLoadersMap()
  {
    return LOADERS_MAP;
  }

  /**
   * Look for implementations for the given class from both classpath and extensions directory, using {@link
   * ServiceLoader}. A user should never put the same two extensions in classpath and extensions directory, if he/she
   * does that, the one that is in the classpath will be loaded, the other will be ignored.
   *
   * @param config       Extensions configuration
   * @param serviceClass The class to look the implementations of (e.g., DruidModule)
   *
   * @return A collection that contains implementations (of distinct concrete classes) of the given class. The order of
   * elements in the returned collection is not specified and not guaranteed to be the same for different calls to
   * getFromExtensions().
   */
  public static <T> Collection<T> getFromExtensions(ExtensionsConfig config, Class<T> serviceClass)
  {
    // It's not clear whether we should recompute modules even if they have been computed already for the serviceClass,
    // but that's how it used to be an preserving the old behaviour here.
    Collection<?> modules = EXTENSIONS_MAP.compute(
        serviceClass,
        (serviceC, ignored) -> new ServiceLoadingFromExtensions<>(config, serviceC).implsToLoad
    );
    //noinspection unchecked
    return (Collection<T>) modules;
  }

  private static class ServiceLoadingFromExtensions<T>
  {
    private final ExtensionsConfig extensionsConfig;
    private final Class<T> serviceClass;
    private final List<T> implsToLoad = new ArrayList<>();
    private final Set<String> implClassNamesToLoad = new HashSet<>();

    private ServiceLoadingFromExtensions(ExtensionsConfig extensionsConfig, Class<T> serviceClass)
    {
      this.extensionsConfig = extensionsConfig;
      this.serviceClass = serviceClass;
      if (extensionsConfig.searchCurrentClassloader()) {
        addAllFromCurrentClassLoader();
      }
      addAllFromFileSystem();
    }

    private void addAllFromCurrentClassLoader()
    {
      ServiceLoader
          .load(serviceClass, Thread.currentThread().getContextClassLoader())
          .forEach(impl -> tryAdd(impl, "classpath"));
    }

    private void addAllFromFileSystem()
    {
      for (File extension : getExtensionFilesToLoad(extensionsConfig)) {
        log.info("Loading extension [%s] for class [%s]", extension.getName(), serviceClass);
        try {
          final URLClassLoader loader = getClassLoaderForExtension(
              extension,
              extensionsConfig.isUseExtensionClassloaderFirst()
          );
          ServiceLoader.load(serviceClass, loader).forEach(impl -> tryAdd(impl, "local file system"));
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void tryAdd(T serviceImpl, String extensionType)
    {
      final String serviceImplName = serviceImpl.getClass().getName();
      if (serviceImplName == null) {
        log.warn(
            "Implementation [%s] was ignored because it doesn't have a canonical name, "
            + "is it a local or anonymous class?",
            serviceImpl.getClass().getName()
        );
      } else if (!implClassNamesToLoad.contains(serviceImplName)) {
        log.info(
            "Adding implementation [%s] for class [%s] from %s extension",
            serviceImplName,
            serviceClass,
            extensionType
        );
        implClassNamesToLoad.add(serviceImplName);
        implsToLoad.add(serviceImpl);
      }
    }
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
    final LinkedHashSet<String> toLoad = config.getLoadList();
    if (toLoad == null) {
      extensionsToLoad = rootExtensionsDir.listFiles();
    } else {
      int i = 0;
      extensionsToLoad = new File[toLoad.size()];
      for (final String extensionName : toLoad) {
        File extensionDir = new File(extensionName);
        if (!extensionDir.isAbsolute()) {
          extensionDir = new File(rootExtensionsDir, extensionName);
        }

        if (!extensionDir.isDirectory()) {
          throw new ISE(
              "Extension [%s] specified in \"druid.extensions.loadList\" didn't exist!?",
              extensionDir.getAbsolutePath()
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
        throw new ISE("Hadoop dependency [%s] didn't exist!?", versionDir.getAbsolutePath());
      }
      hadoopDependenciesToLoad[i++] = versionDir;
    }
    return hadoopDependenciesToLoad;
  }

  /**
   * @param extension The File instance of the extension we want to load
   *
   * @return a URLClassLoader that loads all the jars on which the extension is dependent
   */
  public static URLClassLoader getClassLoaderForExtension(File extension, boolean useExtensionClassloaderFirst)
  {
    return LOADERS_MAP.computeIfAbsent(
        extension,
        theExtension -> makeClassLoaderForExtension(theExtension, useExtensionClassloaderFirst)
    );
  }

  private static URLClassLoader makeClassLoaderForExtension(
      final File extension,
      final boolean useExtensionClassloaderFirst
  )
  {
    final Collection<File> jars = FileUtils.listFiles(extension, new String[]{"jar"}, false);
    final URL[] urls = new URL[jars.size()];

    try {
      int i = 0;
      for (File jar : jars) {
        final URL url = jar.toURI().toURL();
        log.info("added URL[%s] for extension[%s]", url, extension.getName());
        urls[i++] = url;
      }
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    if (useExtensionClassloaderFirst) {
      return new ExtensionFirstClassLoader(urls, Initialization.class.getClassLoader());
    } else {
      return new URLClassLoader(urls, Initialization.class.getClassLoader());
    }
  }

  public static List<URL> getURLsForClasspath(String cp)
  {
    try {
      String[] paths = cp.split(File.pathSeparator);

      List<URL> urls = new ArrayList<>();
      for (String path : paths) {
        File f = new File(path);
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
          urls.add(new File(path).toURI().toURL());
        }
      }
      return urls;
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
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
        TLSCertificateCheckerModule.class,
        EmitterModule.class,
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class),
        new HttpClientModule("druid.broker.http", EscalatedClient.class),
        new CuratorModule(),
        new AnnouncerModule(),
        new MetricsModule(),
        new SegmentWriteOutMediumModule(),
        new ServerModule(),
        new DruidProcessingConfigModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new ExpressionModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageDruidModule(),
        new JacksonConfigManagerModule(),
        new IndexingServiceDiscoveryModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageDruidModule(),
        new FirehoseModule(),
        new JavaScriptModule(),
        new AuthenticatorModule(),
        new AuthenticatorMapperModule(),
        new EscalatorModule(),
        new AuthorizerModule(),
        new AuthorizerMapperModule(),
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
    private final ModulesConfig modulesConfig;
    private final ObjectMapper jsonMapper;
    private final ObjectMapper smileMapper;
    private final List<Module> modules;

    public ModuleList(Injector baseInjector)
    {
      this.baseInjector = baseInjector;
      this.modulesConfig = baseInjector.getInstance(ModulesConfig.class);
      this.jsonMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
      this.smileMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Smile.class));
      this.modules = new ArrayList<>();
    }

    private List<Module> getModules()
    {
      return Collections.unmodifiableList(modules);
    }

    public void addModule(Object input)
    {
      if (input instanceof DruidModule) {
        if (!checkModuleClass(input.getClass())) {
          return;
        }
        baseInjector.injectMembers(input);
        modules.add(registerJacksonModules(((DruidModule) input)));
      } else if (input instanceof Module) {
        if (!checkModuleClass(input.getClass())) {
          return;
        }
        baseInjector.injectMembers(input);
        modules.add((Module) input);
      } else if (input instanceof Class) {
        if (!checkModuleClass((Class<?>) input)) {
          return;
        }
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

    private boolean checkModuleClass(Class<?> moduleClass)
    {
      String moduleClassName = moduleClass.getName();
      if (moduleClassName != null && modulesConfig.getExcludeList().contains(moduleClassName)) {
        log.info("Not loading module [%s] because it is present in excludeList", moduleClassName);
        return false;
      }
      return true;
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
