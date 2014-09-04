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

package io.druid.guice;

import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.jackson.JacksonModule;

import java.util.List;

/**
 */
public class GuiceInjectors
{
  public static Injector makeStartupInjector()
  {
    return Guice.createInjector(
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
            JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
          }
        }
    );
  }

  public static Injector makeStartupInjectorWithModules(Iterable<Module> modules)
  {
    List<Module> theModules = Lists.newArrayList();
    theModules.add(new DruidGuiceExtensions());
    theModules.add(new JacksonModule());
    theModules.add(new PropertiesModule("runtime.properties"));
    theModules.add(new ConfigModule());
    theModules.add(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);
            JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
          }
        }
    );
    for (Module theModule : modules) {
      theModules.add(theModule);
    }


    return Guice.createInjector(theModules);
  }
}
