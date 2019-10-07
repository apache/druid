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

package org.apache.druid.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.guice.PropertiesModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.InitializationTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StatusResourceTest
{
  @Test
  public void testLoadedModules()
  {

    Collection<DruidModule> modules = ImmutableList.of(new InitializationTest.TestDruidModule());
    List<StatusResource.ModuleVersion> statusResourceModuleList = new StatusResource.Status(modules).getModules();

    Assert.assertEquals("Status should have all modules loaded!", modules.size(), statusResourceModuleList.size());

    for (DruidModule module : modules) {
      String moduleName = module.getClass().getName();

      boolean contains = Boolean.FALSE;
      for (StatusResource.ModuleVersion version : statusResourceModuleList) {
        if (version.getName().equals(moduleName)) {
          contains = Boolean.TRUE;
        }
      }
      Assert.assertTrue("Status resource should contain module " + moduleName, contains);
    }
  }

  @Test
  public void testPropertiesWithRestrictedConfigs()
  {
    Injector injector = Guice.createInjector(Collections.singletonList(new PropertiesModule(Collections.singletonList(
        "status.resource.test.runtime.properties"))));
    Map<String, String> returnedProperties = injector.getInstance(StatusResource.class).getProperties();
    Set<String> hiddenProperties = new HashSet<>();
    Splitter.on(",").split(returnedProperties.get("druid.server.hiddenProperties")).forEach(hiddenProperties::add);
    hiddenProperties.forEach((property) -> Assert.assertNull(returnedProperties.get(property)));
  }
}
