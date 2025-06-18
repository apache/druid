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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.inject.Inject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Manages the loading of Druid extensions. Used in two cases: for
 * CLI extensions from {@code Main}, and for {@code DruidModule}
 * extensions during initialization. The design, however, should support
 * any kind of extension that may be needed in the future.
 * The extensions are cached so that they can be reported by various REST APIs.
 */
@LazySingleton
public class ExtensionsLoader
{
  private static final Logger log = new Logger(ExtensionsLoader.class);
  public static final String DRUID_EXTENSION_DEPENDENCIES_JSON = "druid-extension-dependencies.json";
  private final ExtensionsConfig extensionsConfig;
  private final ObjectMapper objectMapper;

  @GuardedBy("this")
  private final HashMap<Pair<File, Boolean>, StandardURLClassLoader> loaders = new HashMap<>();

  /**
   * Map of loaded extensions, keyed by class (or interface).
   */
  @GuardedBy("this")
  private final HashMap<Class<?>, Collection<?>> extensions = new HashMap<>();

  @GuardedBy("this")
  @MonotonicNonNull
  private File[] extensionFilesToLoad;

  @Inject
  public ExtensionsLoader(ExtensionsConfig config, ObjectMapper objectMapper)
  {
    this.extensionsConfig = config;
    this.objectMapper = objectMapper;
  }

  public static ExtensionsLoader instance(Injector injector)
  {
    return injector.getInstance(ExtensionsLoader.class);
  }

  public ExtensionsConfig config()
  {
    return extensionsConfig;
  }

  /**
   * Returns a collection of implementations loaded.
   *
   * @param clazz service class
   * @param <T>   the service type
   */
  public <T> Collection<T> getLoadedImplementations(Class<T> clazz)
  {
    synchronized (this) {
      @SuppressWarnings("unchecked")
      Collection<T> retVal = (Collection<T>) extensions.get(clazz);
      if (retVal == null) {
        return Collections.emptySet();
      }
      return retVal;
    }
  }

  /**
   * @return a collection of implementations loaded.
   */
  public Collection<DruidModule> getLoadedModules()
  {
    return getLoadedImplementations(DruidModule.class);
  }

  @VisibleForTesting
  public Map<Pair<File, Boolean>, StandardURLClassLoader> getLoadersMap()
  {
    synchronized (this) {
      return loaders;
    }
  }

  /**
   * Look for implementations for the given class from both classpath and extensions directory, using {@link
   * ServiceLoader}. A user should never put the same two extensions in classpath and extensions directory, if he/she
   * does that, the one that is in the classpath will be loaded, the other will be ignored.
   *
   * @param serviceClass The class to look the implementations of (e.g., DruidModule)
   *
   * @return A collection that contains implementations (of distinct concrete classes) of the given class. The order of
   * elements in the returned collection is not specified and not guaranteed to be the same for different calls to
   * getFromExtensions().
   */
  @SuppressWarnings("unchecked")
  public <T> Collection<T> getFromExtensions(Class<T> serviceClass)
  {
    // Classes are loaded once upon first request. Since the class path does
    // not change during a run, the set of extension classes cannot change once
    // computed.
    //
    // In practice, it appears the only place this matters is with DruidModule:
    // initialization gets the list of extensions, and two REST API calls later
    // ask for the same list.
    synchronized (this) {
      Collection<?> modules = extensions.computeIfAbsent(
          serviceClass,
          serviceC -> new ServiceLoadingFromExtensions<>(serviceC).implsToLoad
      );
      //noinspection unchecked
      return (Collection<T>) modules;
    }
  }

  public Collection<DruidModule> getModules()
  {
    return getFromExtensions(DruidModule.class);
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
   * @return an array of druid extension files that will be loaded by druid process
   */
  public void initializeExtensionFilesToLoad()
  {
    final File rootExtensionsDir = new File(extensionsConfig.getDirectory());
    if (rootExtensionsDir.exists() && !rootExtensionsDir.isDirectory()) {
      throw new ISE("Root extensions directory [%s] is not a directory!?", rootExtensionsDir);
    }
    File[] extensionsToLoad;
    final LinkedHashSet<String> toLoad = extensionsConfig.getLoadList();
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
    synchronized (this) {
      extensionFilesToLoad = extensionsToLoad == null ? new File[]{} : extensionsToLoad;
    }
  }

  public File[] getExtensionFilesToLoad()
  {
    synchronized (this) {
      if (extensionFilesToLoad == null) {
        initializeExtensionFilesToLoad();
      }
      return extensionFilesToLoad;
    }
  }

  /**
   * @param extension The File instance of the extension we want to load
   *
   * @return a StandardURLClassLoader that loads all the jars on which the extension is dependent
   */
  public StandardURLClassLoader getClassLoaderForExtension(File extension, boolean useExtensionClassloaderFirst)
  {
    return getClassLoaderForExtension(extension, useExtensionClassloaderFirst, new ArrayList<>());
  }

  /**
   * @param extension The File instance of the extension we want to load
   * @param useExtensionClassloaderFirst Whether to return a StandardURLClassLoader that checks extension classloaders first for classes
   * @param extensionDependencyStack If the extension is requested as a dependency of another extension, a list containing the
   *                                 dependency stack of the dependent extension (for checking circular dependencies). Otherwise
   *                                 this is a empty list.
   * @return a StandardURLClassLoader that loads all the jars on which the extension is dependent
   */
  public StandardURLClassLoader getClassLoaderForExtension(File extension, boolean useExtensionClassloaderFirst, List<String> extensionDependencyStack)
  {
    Pair<File, Boolean> classLoaderKey = Pair.of(extension, useExtensionClassloaderFirst);
    synchronized (this) {
      StandardURLClassLoader classLoader = loaders.get(classLoaderKey);
      if (classLoader == null) {
        classLoader = makeClassLoaderWithDruidExtensionDependencies(extension, useExtensionClassloaderFirst, extensionDependencyStack);
        loaders.put(classLoaderKey, classLoader);
      }

      return classLoader;
    }
  }

  private StandardURLClassLoader makeClassLoaderWithDruidExtensionDependencies(File extension, boolean useExtensionClassloaderFirst, List<String> extensionDependencyStack)
  {
    Optional<DruidExtensionDependencies> druidExtensionDependenciesOptional = getDruidExtensionDependencies(extension);
    List<String> druidExtensionDependenciesList = druidExtensionDependenciesOptional.isPresent()
        ? druidExtensionDependenciesOptional.get().getDependsOnDruidExtensions()
        : ImmutableList.of();

    List<ClassLoader> extensionDependencyClassLoaders = new ArrayList<>();
    for (String druidExtensionDependencyName : druidExtensionDependenciesList) {
      Optional<File> extensionDependencyFileOptional = Arrays.stream(getExtensionFilesToLoad())
          .filter(file -> file.getName().equals(druidExtensionDependencyName))
          .findFirst();
      if (!extensionDependencyFileOptional.isPresent()) {
        throw new RE(
            StringUtils.format(
                "Extension [%s] depends on [%s] which is not a valid extension or not loaded.",
                extension.getName(),
                druidExtensionDependencyName
            )
        );
      }
      File extensionDependencyFile = extensionDependencyFileOptional.get();
      if (extensionDependencyStack.contains(extensionDependencyFile.getName())) {
        extensionDependencyStack.add(extensionDependencyFile.getName());
        throw new RE(
            StringUtils.format(
                "Extension [%s] has a circular druid extension dependency. Dependency stack [%s].",
                extensionDependencyStack.get(0),
                extensionDependencyStack
            )
        );
      }
      extensionDependencyStack.add(extensionDependencyFile.getName());
      extensionDependencyClassLoaders.add(
          getClassLoaderForExtension(extensionDependencyFile, useExtensionClassloaderFirst, extensionDependencyStack)
      );
    }

    return makeClassLoaderForExtension(extension, useExtensionClassloaderFirst, extensionDependencyClassLoaders);
  }

  private static StandardURLClassLoader makeClassLoaderForExtension(
      final File extension,
      final boolean useExtensionClassloaderFirst,
      final List<ClassLoader> extensionDependencyClassLoaders
  )
  {
    final Collection<File> jars = FileUtils.listFiles(extension, new String[]{"jar"}, false);
    final URL[] urls = new URL[jars.size()];

    try {
      int i = 0;
      for (File jar : jars) {
        final URL url = jar.toURI().toURL();
        log.debug("added URL [%s] for extension [%s]", url, extension.getName());
        urls[i++] = url;
      }
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    if (useExtensionClassloaderFirst) {
      return new ExtensionFirstClassLoader(urls, ExtensionsLoader.class.getClassLoader(), extensionDependencyClassLoaders);
    } else {
      return new StandardURLClassLoader(urls, ExtensionsLoader.class.getClassLoader(), extensionDependencyClassLoaders);
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

  private Optional<DruidExtensionDependencies> getDruidExtensionDependencies(File extension)
  {
    final Collection<File> jars = FileUtils.listFiles(extension, new String[]{"jar"}, false);
    DruidExtensionDependencies druidExtensionDependencies = null;
    String druidExtensionDependenciesJarName = null;
    for (File extensionFile : jars) {
      try (JarFile jarFile = new JarFile(extensionFile.getPath())) {
        Enumeration<JarEntry> entries = jarFile.entries();

        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          String entryName = entry.getName();
          if (DRUID_EXTENSION_DEPENDENCIES_JSON.equals(entryName)) {
            log.debug("Found extension dependency entry in jar [%s]", extensionFile.getPath());
            if (druidExtensionDependenciesJarName != null) {
              throw new RE(
                  StringUtils.format(
                      "The extension [%s] has multiple jars [%s] [%s] with dependencies in them. Each jar should be in a separate extension directory.",
                      extension.getName(),
                      druidExtensionDependenciesJarName,
                      jarFile.getName()
                  )
              );
            }
            druidExtensionDependencies = objectMapper.readValue(
                jarFile.getInputStream(entry),
                DruidExtensionDependencies.class
            );
            druidExtensionDependenciesJarName = jarFile.getName();
          }
        }
      }
      catch (IOException e) {
        throw new RE(e, "Failed to get dependencies for extension [%s]", extension);
      }
    }
    return druidExtensionDependencies == null ? Optional.empty() : Optional.of(druidExtensionDependencies);
  }

  private class ServiceLoadingFromExtensions<T>
  {
    private final boolean isSimulation;
    private final Class<T> serviceClass;
    private final List<T> implsToLoad = new ArrayList<>();
    private final Set<String> implClassNamesToLoad = new HashSet<>();

    private ServiceLoadingFromExtensions(Class<T> serviceClass)
    {
      this.isSimulation = extensionsConfig.getModulesForSimulation() != null;
      if (isSimulation) {
        log.warn(
            "Running service in simulation testing mode. This is an unsafe test-only"
            + " mode and must never be used in a production cluster."
            + " Remove property[druid.extensions.modulesForSimulation] to disable simulation mode."
        );
      }

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
      for (File extension : getExtensionFilesToLoad()) {
        log.debug("Loading extension [%s] for class [%s]", extension.getName(), serviceClass);
        try {
          final StandardURLClassLoader loader = getClassLoaderForExtension(
              extension,
              extensionsConfig.isUseExtensionClassloaderFirst()
          );
          log.info(
              "Loading extension [%s], jars: %s. Druid extension dependencies [%s]",
              extension.getName(),
              Arrays.stream(loader.getURLs())
                    .map(u -> new File(u.getPath()).getName())
                    .collect(Collectors.joining(", ")),
              loader.getExtensionDependencyClassLoaders()
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
            "Implementation %s was ignored because it doesn't have a canonical name, "
            + "is it a local or anonymous class?",
            serviceImpl.getClass().getName()
        );
      } else if (isSimulation && !extensionsConfig.getModulesForSimulation().contains(serviceImplName)) {
        log.warn(
            "Kashif: Skipping extension[%s] as it is not listed in config[%s]",
            serviceImplName, extensionsConfig.getModulesForSimulation()
        );
      } else if (!implClassNamesToLoad.contains(serviceImplName)) {
        log.debug(
            "Adding implementation %s for class %s from %s extension",
            serviceImplName,
            serviceClass,
            extensionType
        );
        implClassNamesToLoad.add(serviceImplName);
        implsToLoad.add(serviceImpl);
      }
    }
  }
}
