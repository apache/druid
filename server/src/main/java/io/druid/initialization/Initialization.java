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

package io.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.FirehoseModule;
import io.druid.guice.IndexingServiceDiscoveryModule;
import io.druid.guice.JacksonConfigManagerModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.guice.MetadataConfigModule;
import io.druid.guice.ParsersModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StorageNodeModule;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.HttpClientModule;
import io.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.jetty.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import io.tesla.aether.Repository;
import io.tesla.aether.TeslaAether;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);
  private static final Map<String, URLClassLoader> loadersMap = Maps.newHashMap();

  private static final Set<String> exclusions = Sets.newHashSet(
      "io.druid",
      "com.metamx.druid"
  );
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

  /**
   * Used for testing only
   */
  static void clearLoadedModules()
  {
    extensionsMap.clear();
  }

  /**
   * Used for testing only
   */
  static Map<String, URLClassLoader> getLoadersMap()
  {
    return loadersMap;
  }

  /**
   * Look for extension modules for the given class from both classpath and druid.extensions.coordinates.
   * Extensions explicitly specified in druid.extensions.coordinates will be loaded first, if there is a duplicate
   * extension from classpath, it will be ignored.
   *
   * @param config Extensions configuration
   * @param clazz  The class of extension module (e.g., DruidModule)
   *
   * @return A collection that contains distinct extension modules
   */
  public synchronized static <T> Collection<T> getFromExtensions(ExtensionsConfig config, Class<T> clazz)
  {
    final TeslaAether aether = getAetherClient(config);
    final Set<T> retVal = Sets.newHashSet();
    final Set<String> extensionNames = Sets.newHashSet();

    for (String coordinate : config.getCoordinates()) {
      log.info("Loading extension[%s] for class[%s]", coordinate, clazz.getName());
      try {
        URLClassLoader loader = getClassLoaderForCoordinates(aether, coordinate, config.getDefaultVersion());

        for (T module : ServiceLoader.load(clazz, loader)) {
          String moduleName = module.getClass().getCanonicalName();
          if (moduleName == null) {
            log.warn(
                "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
                module.getClass().getName()
            );
          } else if (!extensionNames.contains(moduleName)) {
            log.info("Adding remote extension module[%s] for class[%s]", moduleName, clazz.getName());
            extensionNames.add(moduleName);
            retVal.add(module);
          }
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    if (config.searchCurrentClassloader()) {
      for (T module : ServiceLoader.load(clazz, Initialization.class.getClassLoader())) {
        String moduleName = module.getClass().getCanonicalName();
        if (moduleName == null) {
          log.warn(
              "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
              module.getClass().getName()
          );
        } else if (!extensionNames.contains(moduleName)) {
          log.info("Adding local extension module[%s] for class[%s]", moduleName, clazz.getName());
          extensionNames.add(moduleName);
          retVal.add(module);
        }
      }
    }

    // update the map with currently loaded modules
    extensionsMap.put(clazz, retVal);

    return retVal;
  }

  public static URLClassLoader getClassLoaderForCoordinates(
      TeslaAether aether,
      String coordinate,
      String defaultVersion
  )
      throws DependencyResolutionException, MalformedURLException
  {
    URLClassLoader loader = loadersMap.get(coordinate);
    if (loader == null) {
      final CollectRequest collectRequest = new CollectRequest();

      DefaultArtifact versionedArtifact;
      try {
        // this will throw an exception if no version is specified
        versionedArtifact = new DefaultArtifact(coordinate);
      }
      catch (IllegalArgumentException e) {
        // try appending the default version so we can specify artifacts without versions
        if (defaultVersion != null) {
          versionedArtifact = new DefaultArtifact(coordinate + ":" + defaultVersion);
        } else {
          throw e;
        }
      }

      collectRequest.setRoot(new Dependency(versionedArtifact, JavaScopes.RUNTIME));
      DependencyRequest dependencyRequest = new DependencyRequest(
          collectRequest,
          DependencyFilterUtils.andFilter(
              DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME),
              new DependencyFilter()
              {
                @Override
                public boolean accept(DependencyNode node, List<DependencyNode> parents)
                {
                  if (accept(node.getArtifact())) {
                    return false;
                  }

                  for (DependencyNode parent : parents) {
                    if (accept(parent.getArtifact())) {
                      return false;
                    }
                  }

                  return true;
                }

                private boolean accept(final Artifact artifact)
                {
                  return exclusions.contains(artifact.getGroupId());
                }
              }
          )
      );

      try {
        final List<Artifact> artifacts = aether.resolveArtifacts(dependencyRequest);

        List<URL> urls = Lists.newArrayListWithExpectedSize(artifacts.size());
        for (Artifact artifact : artifacts) {
          if (!exclusions.contains(artifact.getGroupId())) {
            urls.add(artifact.getFile().toURI().toURL());
          } else {
            log.debug("Skipped Artifact[%s]", artifact);
          }
        }

        for (URL url : urls) {
          log.info("Added URL[%s]", url);
        }

        loader = new URLClassLoader(urls.toArray(new URL[urls.size()]), Initialization.class.getClassLoader());
        loadersMap.put(coordinate, loader);
      }
      catch (Exception e) {
        log.error(e, "Unable to resolve artifacts for [%s].", dependencyRequest);
        throw Throwables.propagate(e);
      }
    }
    return loader;
  }

  public static DefaultTeslaAether getAetherClient(ExtensionsConfig config)
  {
    /*
    DefaultTeslaAether logs a bunch of stuff to System.out, which is annoying.  We choose to disable that
    unless debug logging is turned on.  "Disabling" it, however, is kinda bass-ackwards.  We copy out a reference
    to the current System.out, and set System.out to a noop output stream.  Then after DefaultTeslaAether has pulled
    The reference we swap things back.

    This has implications for other things that are running in parallel to this.  Namely, if anything else also grabs
    a reference to System.out or tries to log to it while we have things adjusted like this, then they will also log
    to nothingness.  Fortunately, the code that calls this is single-threaded and shouldn't hopefully be running
    alongside anything else that's grabbing System.out.  But who knows.
    */

    List<String> remoteUriList = config.getRemoteRepositories();

    List<Repository> remoteRepositories = Lists.newArrayList();
    for (String uri : remoteUriList) {
      try {
        URI u = new URI(uri);
        Repository r = new Repository(uri);

        if (u.getUserInfo() != null) {
          String[] auth = u.getUserInfo().split(":", 2);
          if (auth.length == 2) {
            r.setUsername(auth[0]);
            r.setPassword(auth[1]);
          } else {
            log.warn(
                "Invalid credentials in repository URI, expecting [<user>:<password>], got [%s] for [%s]",
                u.getUserInfo(),
                uri
            );
          }
        }
        remoteRepositories.add(r);
      }
      catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }

    if (log.isTraceEnabled() || log.isDebugEnabled()) {
      return new DefaultTeslaAether(
          config.getLocalRepository(),
          remoteRepositories.toArray(new Repository[remoteRepositories.size()])
      );
    }

    PrintStream oldOut = System.out;
    try {
      System.setOut(
          new PrintStream(
              new OutputStream()
              {
                @Override
                public void write(int b) throws IOException
                {

                }

                @Override
                public void write(byte[] b) throws IOException
                {

                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException
                {

                }
              }
              , false, StringUtils.UTF8_STRING
          )
      );
      return new DefaultTeslaAether(
          config.getLocalRepository(),
          remoteRepositories.toArray(new Repository[remoteRepositories.size()])
      );
    }
    catch (UnsupportedEncodingException e) {
      // should never happen
      throw new IllegalStateException(e);
    }
    finally {
      System.setOut(oldOut);
    }
  }

  public static Injector makeInjectorWithModules(final Injector baseInjector, Iterable<? extends Module> modules)
  {
    final ModuleList defaultModules = new ModuleList(baseInjector);
    defaultModules.addModules(
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
        new LocalDataStorageDruidModule(),
        new FirehoseModule(),
        new ParsersModule()
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
