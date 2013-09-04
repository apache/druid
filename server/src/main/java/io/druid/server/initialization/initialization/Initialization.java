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

package io.druid.server.initialization.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.guice.DruidGuiceExtensions;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.DruidModule;
import io.druid.jackson.JacksonModule;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.eclipse.aether.artifact.Artifact;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);

  private static final List<String> exclusions = Arrays.asList(
      "io.druid",
      "com.metamx.druid"
  );


  public static Injector makeInjector(final Object... modules)
  {
    final List<Class<?>> externalModules = Lists.newArrayList();
    final DefaultTeslaAether aether = new DefaultTeslaAether();
    try {
      final List<Artifact> artifacts = aether.resolveArtifacts(
          "com.metamx.druid-extensions-mmx:druid-extensions:0.4.18-SNAPSHOT"
      );
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

      ClassLoader loader = new URLClassLoader(
          urls.toArray(new URL[urls.size()]), Initialization.class.getClassLoader()
      );

      externalModules.add(loader.loadClass("com.metamx.druid.extensions.query.topn.TopNQueryDruidModule"));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final Injector baseInjector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule("runtime.properties"),
        new ConfigModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);

            for (Object module : modules) {
              if (module instanceof Class) {
                binder.bind((Class) module);
              }
            }

            for (Class<?> externalModule : externalModules) {
              binder.bind(externalModule);
            }
          }
        }
    );


    ModuleList actualModules = new ModuleList(baseInjector);
    actualModules.addModule(DruidSecondaryModule.class);
    for (Object module : modules) {
      actualModules.addModule(module);
    }

    for (Class<?> externalModule : externalModules) {
      actualModules.addModule(externalModule);
    }

    return Guice.createInjector(actualModules.getModules());
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
