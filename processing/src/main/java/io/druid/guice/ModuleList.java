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
