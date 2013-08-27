/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.ISE;
import com.metamx.druid.guice.DruidGuiceExtensions;
import com.metamx.druid.guice.DruidSecondaryModule;
import com.metamx.druid.guice.annotations.Json;
import com.metamx.druid.guice.annotations.Smile;
import com.metamx.druid.jackson.JacksonModule;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 */
public class Initialization
{
  public static Injector makeInjector(final Object... modules)
  {
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
          }
        }
    );

    List<Object> actualModules = Lists.newArrayList();

    actualModules.add(DruidSecondaryModule.class);
    actualModules.addAll(Arrays.asList(modules));

    return Guice.createInjector(
        Lists.transform(
            actualModules,
            new Function<Object, Module>()
            {
              ObjectMapper jsonMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
              ObjectMapper smileMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Smile.class));

              @Override
              @SuppressWarnings("unchecked")
              public Module apply(@Nullable Object input)
              {
                if (input instanceof DruidModule) {
                  baseInjector.injectMembers(input);
                  return registerJacksonModules(((DruidModule) input));
                }

                if (input instanceof Module) {
                  baseInjector.injectMembers(input);
                  return (Module) input;
                }

                if (input instanceof Class) {
                  if (DruidModule.class.isAssignableFrom((Class) input)) {
                    return registerJacksonModules(baseInjector.getInstance((Class<? extends DruidModule>) input));
                  }
                  if (Module.class.isAssignableFrom((Class) input)) {
                    return baseInjector.getInstance((Class<? extends Module>) input);
                  }
                  else {
                    throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
                  }
                }

                throw new ISE("Unknown module type[%s]", input.getClass());
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
        )
    );
  }
}
