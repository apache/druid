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

package io.druid.testing.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.initialization.Initialization;
import org.testng.IModuleFactory;
import org.testng.ITestContext;

import java.util.Collections;
import java.util.List;

public class DruidTestModuleFactory implements IModuleFactory
{
  private static final Module module = new DruidTestModule();
  private static final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      getModules()
  );

  public static Injector getInjector()
  {
    return injector;
  }

  private static List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidTestModule(),
        new IndexingServiceFirehoseModule()
    );
  }

  @Override
  public Module createModule(ITestContext context, Class<?> testClass)
  {
    context.addGuiceModule(DruidTestModule.class, module);
    context.addInjector(Collections.singletonList(module), injector);
    return module;
  }

}
