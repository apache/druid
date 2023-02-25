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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CollectionUtils;

import javax.inject.Inject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages the loading of Druid extensions. Used in two cases: for
 * CLI extensions from {@code Main}, and for {@code DruidModule}
 * extensions during initialization. The design, however, should support
 * any kind of extension that may be needed in the future.
 * The extensions are cached so that they can be reported by various REST APIs.
 * <p>
 * Extensions reside in a directory. The name of the directory is the extension
 * name. No two extensions can have the same name. This is not actually a restriction
 * as extensions reside in {@code $DRUID_HOME/extensions}, so each extension must
 * be in its own extension. The extension name is the same as that used in the
 * Druid extension load list.
 */
@LazySingleton
public class ExtensionsLoader
{
  private static final Logger log = new Logger(ExtensionsLoader.class);

  private final ExtensionsConfig extensionsConfig;
  private final ConcurrentHashMap<String, URLClassLoader> loaders = new ConcurrentHashMap<>();

  /**
   * Map of loaded extensions, keyed by class (or interface).
   */
  private final ConcurrentHashMap<Class<?>, Collection<?>> extensions = new ConcurrentHashMap<>();

  @Inject
  public ExtensionsLoader(ExtensionsConfig config)
  {
    this.extensionsConfig = config;
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
    @SuppressWarnings("unchecked")
    Collection<T> retVal = (Collection<T>) extensions.get(clazz);
    if (retVal == null) {
      return Collections.emptySet();
    }
    return retVal;
  }

  /**
   * @return a collection of implementations loaded.
   */
  public Collection<DruidModule> getLoadedModules()
  {
    return getLoadedImplementations(DruidModule.class);
  }

  @VisibleForTesting
  public Map<String, URLClassLoader> getLoadersMap()
  {
    return loaders;
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
    Collection<?> modules = extensions.computeIfAbsent(
        serviceClass,
        serviceC -> new ServiceLoadingFromExtensions<>(serviceC).implsToLoad
    );
    //noinspection unchecked
    return (Collection<T>) modules;
  }

  public Collection<DruidModule> getModules()
  {
    return getFromExtensions(DruidModule.class);
  }

  /**
   * Find all the extension files that should be loaded by Druid.
   * <p/>
   * If user explicitly specifies {@code druid.extensions.loadList}, then it will look for those extensions under root
   * extensions directory. If one of them is not found, Druid will fail loudly.
   * <p/>
   * If user doesn't specify {@code druid.extensions.loadList} (or its value is empty), druid will load all the extensions
   * under the root extensions directory.
   *
   * @return an array of Druid extension files that will be loaded by Druid process
   */
  public List<File> getExtensionFilesToLoad()
  {
    List<File> extensionsPath = new ArrayList<>();
    final LinkedHashSet<String> toLoad = extensionsConfig.getLoadList();
    List<String> path = extensionsConfig.getPath();

    // For backward compatibility, we allow the extensions directory to be set to
    // its default value, but to not exist. This is a bit
    // of hack: a better solution is to require that the directory exist if configured.
    // But, since a missing config means (use the default), we can't require the user
    // to use the workaround which is to set the directory to an empty string. Experienced
    // users can use that feature, but newbies won't have a clue. Also, pull-deps and other
    // tools count on the old behavior.
    // Note that, in the case of a non-existent directory, any extensions have to be
    // an absolute path, on the extensions path, or built-in. The result is a much more
    // confusing error than just saying, "hey, your extensions directory is wrong!"
    String extensionsDir = extensionsConfig.getDirectory();
    if (!Strings.isNullOrEmpty(extensionsDir)) {
      final File rootExtensionsDir = new File(extensionsDir);
      if (rootExtensionsDir.isDirectory()) {
        // Verify the directory is readable.
        verifyDirectory("Extensions", rootExtensionsDir);
        extensionsPath.add(rootExtensionsDir);
      // Verify a non-default path, but only if there are extensions. PullDeps will
      // create the directory, but other tools require that the path exists. We count on
      // the fact that, for LoadDeps, the load list will be empty, while for a Druid server,
      // it is very likely to be set. This DOES NOT handle the corner case in which the user
      // wants to load all extensions, but messes up their extension directory. To fix that,
      // PullDeps should require that the extension directory exists, which does not occur
      // today in the distribution project.
      } else if (!ExtensionsConfig.DEFAULT_EXTENSIONS_DIR.equals(extensionsDir) &&
                 !CollectionUtils.isNullOrEmpty(toLoad)) {
        // Non-default value. It must exist. Fail since it doesn't.
        verifyDirectory("Extensions", rootExtensionsDir);
      }
    }
    if (!CollectionUtils.isNullOrEmpty(path)) {
      // Validate the paths
      for (String extnDir : path) {
        final File rootExtensionsDir = new File(extnDir);
        // Breaking change: we require that the directory exists. Prior versions didn't
        // check unless there we actually extensions loaded, which meant that if the loadList
        // was empty, the user got no extensions, rather than the standard ones.
        // This more strict approach is safer.
        verifyDirectory("Extensions", rootExtensionsDir);
        extensionsPath.add(rootExtensionsDir);
      }
    }

    List<File> extensionsToLoad = new ArrayList<>();
    if (toLoad == null) {
      // No load list? Load everything in all along the path.
      for (File extnDir : extensionsPath) {
        extensionsToLoad.addAll(Arrays.asList(extnDir.listFiles()));
      }
    } else {
      // Resolve extensions one by one, searching the path for each
      for (final String extensionName : toLoad) {
        File extensionDir = new File(extensionName);
        if (extensionDir.isAbsolute()) {
          // The path is absolute: no need to search
          verifyDirectory("Extension", extensionDir);
          extensionsToLoad.add(extensionDir);
        } else {
          // Search the path. The extension must exist.
          boolean found = false;
          for (File rootExtensionsDir : extensionsPath) {
            extensionDir = new File(rootExtensionsDir, extensionName);
            if (extensionDir.exists()) {
              verifyDirectory("Extension", extensionDir);
              extensionsToLoad.add(extensionDir);
              found = true;
              break;
            }
          }

          if (!found) {
            throw new ISE(
                "Extension [%s] specified in \"druid.extensions.loadList\" not found in the extension path",
                extensionDir.getAbsolutePath()
            );
          }
        }
      }
    }
    return extensionsToLoad;
  }

  private void verifyDirectory(String label, File dir)
  {
    if (!dir.isDirectory()) {
      throw new ISE("%s directory [%s] is not a directory", label, dir);
    }
    if (!dir.canRead()) {
      throw new ISE("%s directory [%s] is not readable", label, dir);
    }
  }

  /**
   * @param extension The File instance of the extension we want to load
   *
   * @return a URLClassLoader that loads all the jars on which the extension is dependent
   */
  public URLClassLoader getClassLoaderForExtension(File extension)
  {
    return getClassLoaderForExtension(extension, extensionsConfig.isUseExtensionClassloaderFirst());
  }

  public URLClassLoader getClassLoaderForExtension(File extension, boolean useExtensionClassloaderFirst)
  {
    return loaders.computeIfAbsent(
        extension.getName(),
        k -> makeClassLoaderForExtension(extension, useExtensionClassloaderFirst)
    );
  }

  private URLClassLoader makeClassLoaderForExtension(
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
        log.debug("added URL [%s] for extension [%s]", url, extension.getName());
        urls[i++] = url;
      }
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    if (useExtensionClassloaderFirst) {
      return new ExtensionFirstClassLoader(urls, ExtensionsLoader.class.getClassLoader());
    } else {
      return new URLClassLoader(
          // extension.getName() - after moving off of Java 8
          urls,
          ExtensionsLoader.class.getClassLoader()
      );
    }
  }

  /**
   * Obtains the class loader for the given extension name. Assumes the extension
   * has been loaded. This method primarily solves the use case of resolving a class
   * within jars in an extension, when run from an IDE. When run in production, the
   * class loader of the extensions {@code DruidModule} will give the extension class
   * loader. But, when run in an IDE, the extension may be on the class path, so that
   * the {@code AppClassLoader} finds the class before the extension class loader does.
   * In this case, classes from the extension report the {@code AppClassLoader}, not
   * the extensions {@link URLClassLoader} as their class loader. When this happens,
   * an attempt to find a class using the class loader will fail. This method is a workaround:
   * even in an IDE, we use the actual extension class loader.
   */
  public URLClassLoader getClassLoaderForExtension(String extensionName)
  {
    return loaders.get(extensionName);
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

  private class ServiceLoadingFromExtensions<T>
  {
    private final Class<T> serviceClass;
    private final List<T> implsToLoad = new ArrayList<>();
    private final Set<String> implClassNamesToLoad = new HashSet<>();

    private ServiceLoadingFromExtensions(Class<T> serviceClass)
    {
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
          final URLClassLoader loader = getClassLoaderForExtension(extension);

          log.info(
              "Loading extension [%s], jars: %s",
              extension.getName(),
              Arrays.stream(loader.getURLs())
                    .map(u -> new File(u.getPath()).getName())
                    .collect(Collectors.joining(", "))
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
