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

package io.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.ISE;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

/**
 */
public class ModuleList
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

  public List<Module> getModules()
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
