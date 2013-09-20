/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.DataSegmentPusherPullerModule;
import io.druid.guice.DbConnectorModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.IndexingServiceDiscoveryModule;
import io.druid.guice.JacksonConfigManagerModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StorageNodeModule;
import io.druid.guice.TaskLogsModule;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.DruidModule;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.ExtensionsConfig;
import io.druid.server.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import io.tesla.aether.TeslaAether;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
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
  private static final Map<String, ClassLoader> loadersMap = Maps.newHashMap();

  private static final Set<String> exclusions = Sets.newHashSet(
      "io.druid",
      "com.metamx.druid"
  );

  public synchronized static <T> List<T> getFromExtensions(ExtensionsConfig config, Class<T> clazz)
  {
    final TeslaAether aether = getAetherClient(config);
    List<T> retVal = Lists.newArrayList();

    for (String coordinate : config.getCoordinates()) {
      log.info("Loading extension[%s]", coordinate);
      try {
        ClassLoader loader = loadersMap.get(coordinate);
        if (loader == null) {
          final CollectRequest collectRequest = new CollectRequest();
          collectRequest.setRoot(new Dependency(new DefaultArtifact(coordinate), JavaScopes.RUNTIME));
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

          final List<Artifact> artifacts = aether.resolveArtifacts(dependencyRequest);
          List<URL> urls = Lists.newArrayListWithExpectedSize(artifacts.size());
          for (Artifact artifact : artifacts) {
            if (!exclusions.contains(artifact.getGroupId())) {
              urls.add(artifact.getFile().toURI().toURL());
            }
            else {
              log.error("Skipped Artifact[%s]", artifact);
            }
          }

          for (URL url : urls) {
            log.error("Added URL[%s]", url);
          }

          loader = new URLClassLoader(urls.toArray(new URL[urls.size()]), Initialization.class.getClassLoader());
          loadersMap.put(coordinate, loader);
        }

        final ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz, loader);

        for (T module : serviceLoader) {
          log.info("Adding extension module[%s]", module.getClass());
          retVal.add(module);
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    return retVal;
  }

  private static DefaultTeslaAether getAetherClient(ExtensionsConfig config)
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
    if (log.isTraceEnabled() || log.isDebugEnabled()) {
      return new DefaultTeslaAether(config.getLocalRepository(), config.getRemoteRepositories());
    }

    PrintStream oldOut = System.out;
    try {
      System.setOut(new PrintStream(ByteStreams.nullOutputStream()));
      return new DefaultTeslaAether(config.getLocalRepository(), config.getRemoteRepositories());
    }
    finally {
      System.setOut(oldOut);
    }
  }

  public static Injector makeInjectorWithModules(final Injector baseInjector, Iterable<Object> modules)
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
        new DbConnectorModule(),
        new JacksonConfigManagerModule(),
        new IndexingServiceDiscoveryModule(),
        new DataSegmentPusherPullerModule(),
        new TaskLogsModule()
    );

    ModuleList actualModules = new ModuleList(baseInjector);
    actualModules.addModule(DruidSecondaryModule.class);
    for (Object module : modules) {
      actualModules.addModule(module);
    }

    final ExtensionsConfig config = baseInjector.getInstance(ExtensionsConfig.class);
    for (DruidModule module : Initialization.getFromExtensions(config, DruidModule.class)) {
      actualModules.addModule(module);
    }

    return Guice.createInjector(Modules.override(defaultModules.getModules()).with(actualModules.getModules()));
  }

  private static class ModuleList
  {
    private final Injector baseInjector;
    private final ObjectMapper jsonMapper;
    private final ObjectMapper smileMapper;
    private final List<Module> modules;

    public ModuleList(Injector baseInjector) {
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
      }
      else if (input instanceof Module) {
        baseInjector.injectMembers(input);
        modules.add((Module) input);
      }
      else if (input instanceof Class) {
        if (DruidModule.class.isAssignableFrom((Class) input)) {
          modules.add(registerJacksonModules(baseInjector.getInstance((Class<? extends DruidModule>) input)));
        }
        else if (Module.class.isAssignableFrom((Class) input)) {
          modules.add(baseInjector.getInstance((Class<? extends Module>) input));
          return;
        }
        else {
          throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
        }
      }
      else {
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
