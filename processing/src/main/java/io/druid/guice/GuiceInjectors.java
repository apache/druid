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

package io.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.jackson.JacksonModule;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
public class GuiceInjectors
{
  public static Collection<Module> makeDefaultStartupModules()
  {
    return ImmutableList.<Module>of(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule(Arrays.asList("common.runtime.properties", "runtime.properties")),
        new ConfigModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);
            JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
            JsonConfigProvider.bind(binder, "druid.modules", ModulesConfig.class);
          }
        }
    );
  }

  public static Injector makeStartupInjector()
  {
    return Guice.createInjector(makeDefaultStartupModules());
  }

  public static Injector makeStartupInjectorWithModules(Iterable<? extends Module> modules)
  {
    List<Module> theModules = Lists.newArrayList();
    theModules.addAll(makeDefaultStartupModules());
    for (Module theModule : modules) {
      theModules.add(theModule);
    }
    return Guice.createInjector(theModules);
  }
}
