/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.client.cache;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.initialization.DruidModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

/**
 *
 */
public class CacheConfigTest
{
  static Injector injector;
  static JsonConfigurator configurator;
  JsonConfigProvider<CacheConfig> configProvider;
  private static final String propertyPrefix = "io.druid.test.cache";

  @BeforeClass
  public static void populateStatics(){
    injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.<com.google.inject.Module>of(new CacheConfigTestModule()));
    configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
  }
  private static class CacheConfigTestModule implements DruidModule
  {

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.<Module>of(new SimpleModule());
    }

    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder,propertyPrefix,CacheConfig.class);
    }
  }
  private Properties properties = new Properties();

  @Before
  public void setupTest(){
    properties.clear();
    configProvider = JsonConfigProvider.of(propertyPrefix, CacheConfig.class);
  }

  @Test
  public void testInjection1()
  {
    properties.put(propertyPrefix + ".numBackgroundThreads", "5");
    properties.put(propertyPrefix + ".populateCache", "true");
    properties.put(propertyPrefix + ".useCache", "true");
    properties.put(propertyPrefix + ".unCacheable", "[\"a\",\"b\"]");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();

    injector.injectMembers(config);
    Assert.assertEquals(5, config.getNumBackgroundThreads());
    Assert.assertEquals(true, config.isPopulateCache());
    Assert.assertEquals(true, config.isUseCache());
  }
  @Test
  public void testInjection2()
  {
    properties.put(propertyPrefix + ".numBackgroundThreads", "99");
    properties.put(propertyPrefix + ".populateCache", "false");
    properties.put(propertyPrefix + ".useCache", "false");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();

    Assert.assertEquals(99, config.getNumBackgroundThreads());
    Assert.assertEquals(false, config.isPopulateCache());
    Assert.assertEquals(false, config.isUseCache());
  }

  @Test(expected = com.google.inject.ProvisionException.class)
  public void testValidationError()
  {
    properties.put(propertyPrefix + ".numBackgroundThreads", "-1");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    Assert.assertNotEquals(-1, config.getNumBackgroundThreads());
  }


  @Test(expected = com.google.inject.ProvisionException.class)
  public void testValidationInsaneError()
  {
    properties.put(propertyPrefix + ".numBackgroundThreads", "BABBA YAGA");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }

  @Test(expected = com.google.inject.ProvisionException.class)
  public void testTRUE()
  {
    properties.put(propertyPrefix + ".populateCache", "TRUE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }

  @Test(expected = com.google.inject.ProvisionException.class)
  public void testFALSE()
  {
    properties.put(propertyPrefix + ".populateCache", "FALSE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }


  @Test(expected = com.google.inject.ProvisionException.class)
  public void testFaLse()
  {
    properties.put(propertyPrefix + ".populateCache", "FaLse");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }


}
