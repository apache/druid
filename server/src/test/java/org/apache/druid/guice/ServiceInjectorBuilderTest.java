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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.ServiceInjectorBuilder;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServiceInjectorBuilderTest
{
  @Test
  public void testNormalUsage()
  {
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(new Properties())
        .build();

    CoreInjectorBuilder coreBuilder = new CoreInjectorBuilder(startupInjector);
    coreBuilder.ignoreLoadScopes();
    coreBuilder.addModule(
        new LocalProviderModule(1)
    );

    ServiceInjectorBuilder si = new ServiceInjectorBuilder(coreBuilder);
    si.add(
        new LocalProviderModule(2)
    );
    Injector injector = si.build();
    Integer v = injector.getInstance(Integer.class);
    assertEquals(2, v);
  }

  @Test
  public void testOverrideBelow()
  {
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(new Properties())
        .build();

    CoreInjectorBuilder coreBuilder = new CoreInjectorBuilder(startupInjector);
    coreBuilder.ignoreLoadScopes();
    coreBuilder.addModule(
        Modules.override(new LocalProviderModule(0))
            .with(new LocalProviderModule(1))
    );

    ServiceInjectorBuilder si = new ServiceInjectorBuilder(coreBuilder);
    si.add(
        new LocalProviderModule(2)
    );
    Injector injector = si.build();
    Integer v = injector.getInstance(Integer.class);
    assertEquals(2, v);
  }

  @Test
  public void testOverrideBoth()
  {
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(new Properties())
        .build();

    CoreInjectorBuilder coreBuilder = new CoreInjectorBuilder(startupInjector);
    coreBuilder.ignoreLoadScopes();
    coreBuilder.addModule(
        Modules.override(new LocalProviderModule(0))
            .with(new LocalProviderModule(1))
    );

    ServiceInjectorBuilder si = new ServiceInjectorBuilder(coreBuilder);
    si.add(
        Modules.override(new LocalProviderModule(-1))
            .with(new LocalProviderModule(2))
    );
    Injector injector = si.build();
    Integer v = injector.getInstance(Integer.class);
    assertEquals(2, v);
  }

  static class LocalProviderModule implements DruidModule
  {
    private int val;

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
      return val;
    }
  }
}
