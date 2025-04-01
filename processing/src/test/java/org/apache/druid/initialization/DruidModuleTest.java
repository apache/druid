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

package org.apache.druid.initialization;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DruidModuleTest
{
  @Test
  public void testOverride()
  {
    Injector initInjector = Guice.createInjector(new LocalProviderModule(10));
    DruidModule overrideModule = DruidModule.override(new LocalProviderModule(1), new LocalProviderModule(2));
    initInjector.injectMembers(overrideModule);
    Injector injector = Guice.createInjector(overrideModule);
    Integer val = injector.getInstance(Integer.class);
    assertEquals(12, val);
  }

  @Test
  public void testOverrideJacksonModules()
  {
    Injector initInjector = Guice.createInjector(new LocalProviderModule(10));
    DruidModule overrideModule = DruidModule.override(new LocalProviderModule(1), new LocalProviderModule(2));
    initInjector.injectMembers(overrideModule);
    List<? extends Module> jacksonModules = overrideModule.getJacksonModules();
    // Even thru it was overridden, the base module should still provide these.
    assertEquals(1, jacksonModules.size());
  }

  static class LocalProviderModule implements DruidModule
  {
    private int val;
    private int baseVal = 0;

    @Inject
    public void injectInteger(Integer baseVal)
    {
      this.baseVal = baseVal;
    }

    public LocalProviderModule(int i)
    {
      val = i;
    }

    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    public Integer getVal()
    {
      return val + baseVal;
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      List<? extends Module> li = new ArrayList<>();
      for (int i = 0; i < val; i++) {
        li.add(null);
      }
      return li;
    }

  }

}
