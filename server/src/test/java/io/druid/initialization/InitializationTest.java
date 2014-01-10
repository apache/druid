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

package io.druid.initialization;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import io.druid.server.initialization.ExtensionsConfig;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InitializationTest
{
  private String oldService;
  private String oldHost;
  private String oldPort;

  @Before
  public void messWithSystemProperties()
  {
    // required to test Initialization.makeInjectorWithModules
    oldService = System.setProperty("druid.service", "test-service");
    oldHost = System.setProperty("druid.host", "test-host");
    oldPort = System.setProperty("druid.port", "8080");
  }

  @After
  public void cleanup()
  {
    System.setProperty("druid.service", oldService == null ? "" : oldService);
    System.setProperty("druid.host", oldHost == null ? "" : oldHost);
    System.setProperty("druid.port", oldPort == null ? "" : oldPort);
  }

  @Test
  public void test01InitialModulesEmpty() throws Exception
  {
    Assert.assertEquals(
        "Initial set of loaded modules must be empty",
        0,
        Initialization.getLoadedModules(DruidModule.class).size()
    );
  }

  @Test
  public void test02MakeStartupInjector() throws Exception
  {
    Injector startupInjector = Initialization.makeStartupInjector();
    Assert.assertNotNull(startupInjector);
    Assert.assertNotNull(startupInjector.getInstance(ObjectMapper.class));
  }

  @Test
  public void test03ClassLoaderExtensionsLoading()
  {
    Injector startupInjector = Initialization.makeStartupInjector();

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
  public void test04MakeInjectorWithModules() throws Exception
  {
    Injector startupInjector = Initialization.makeStartupInjector();
    Injector injector = Initialization.makeInjectorWithModules(startupInjector, ImmutableList.of());
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
