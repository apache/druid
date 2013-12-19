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

package io.druid.server;

import static io.druid.server.StatusResource.ModuleVersion;

import com.google.inject.Injector;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

/**
 */
public class StatusResourceTest
{

  private Collection<DruidModule> loadTestModule()
  {
    Injector baseInjector = Initialization.makeStartupInjector();
    return Initialization.getFromExtensions(baseInjector.getInstance(ExtensionsConfig.class), DruidModule.class);
  }

  @Test
  public void testLoadedModules()
  {
    final StatusResource resource = new StatusResource();
    List<ModuleVersion> statusResourceModuleList;

    statusResourceModuleList = resource.doGet().getModules();
    Assert.assertEquals("No Modules should be loaded currently!", statusResourceModuleList.size(), 0);

    Collection<DruidModule> modules = loadTestModule();
    statusResourceModuleList = resource.doGet().getModules();

    Assert.assertEquals("Status should have all modules loaded!", statusResourceModuleList.size(), modules.size());

    for (DruidModule module : modules) {
      String moduleName = module.getClass().getCanonicalName();

      boolean contains = Boolean.FALSE;
      for (ModuleVersion version : statusResourceModuleList) {
        if (version.getName().equals(moduleName)) {
          contains = Boolean.TRUE;
        }
      }
      Assert.assertTrue("Status resource should contains module " + moduleName, contains);
    }
  }

}
