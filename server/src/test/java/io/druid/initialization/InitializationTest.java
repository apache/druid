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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.annotation.Nullable;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InitializationTest
{
  @Test
  public void test01InitialModulesEmpty() throws Exception
  {
    Initialization.clearLoadedModules();
    Assert.assertEquals(
        "Initial set of loaded modules must be empty",
        0,
        Initialization.getLoadedModules(DruidModule.class).size()
    );
  }

  @Test
  public void test02MakeStartupInjector() throws Exception
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();
    Assert.assertNotNull(startupInjector);
    Assert.assertNotNull(startupInjector.getInstance(ObjectMapper.class));
  }

  @Test
  public void test03ClassLoaderExtensionsLoading()
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();

    Function<DruidModule, String> fnClassName = new Function<DruidModule, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable DruidModule input)
      {
        return input.getClass().getCanonicalName();
      }
    };

    Assert.assertFalse(
        "modules does not contain TestDruidModule",
        Collections2.transform(Initialization.getLoadedModules(DruidModule.class), fnClassName)
                    .contains("io.druid.initialization.InitializationTest.TestDruidModule")
    );

    Collection<DruidModule> modules = Initialization.getFromExtensions(
        startupInjector.getInstance(ExtensionsConfig.class),
        DruidModule.class
    );

    Assert.assertTrue(
        "modules contains TestDruidModule",
        Collections2.transform(modules, fnClassName)
                    .contains("io.druid.initialization.InitializationTest.TestDruidModule")
    );
  }

  @Test
  public void test04DuplicateClassLoaderExtensions() throws Exception
  {
    Initialization.getLoadersMap().put("xyz", (URLClassLoader) Initialization.class.getClassLoader());

    Collection<DruidModule> modules = Initialization.getFromExtensions(
        new ExtensionsConfig()
        {
          @Override
          public List<String> getCoordinates()
          {
            return ImmutableList.of("xyz");
          }

          @Override
          public List<String> getRemoteRepositories()
          {
            return ImmutableList.of();
          }
        }, DruidModule.class
    );

    Set<String> loadedModuleNames = Sets.newHashSet();
    for (DruidModule module : modules) {
      Assert.assertFalse("Duplicate extensions are loaded", loadedModuleNames.contains(module.getClass().getName()));
      loadedModuleNames.add(module.getClass().getName());
    }

    Initialization.getLoadersMap().clear();
  }

  @Test
  public void test05MakeInjectorWithModules() throws Exception
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();
    Injector injector = Initialization.makeInjectorWithModules(
        startupInjector, ImmutableList.<com.google.inject.Module>of(
            new com.google.inject.Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                );
              }
            }
        )
    );
    Assert.assertNotNull(injector);
  }

  @Test
  public void testGetLoadedModules()
  {

    Set<DruidModule> modules = Initialization.getLoadedModules(DruidModule.class);

    Set<DruidModule> loadedModules = Initialization.getLoadedModules(DruidModule.class);
    Assert.assertEquals("Set from loaded modules #1 should be same!", modules, loadedModules);

    Set<DruidModule> loadedModules2 = Initialization.getLoadedModules(DruidModule.class);
    Assert.assertEquals("Set from loaded modules #2 should be same!", modules, loadedModules2);
  }

  public static class TestDruidModule implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.of();
    }

    @Override
    public void configure(Binder binder)
    {
      // Do nothing
    }
  }
}
