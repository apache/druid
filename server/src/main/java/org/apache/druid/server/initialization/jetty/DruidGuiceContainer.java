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

package org.apache.druid.server.initialization.jetty;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scope;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.servlet.WebConfig;
import org.apache.druid.guice.DruidScopes;
import org.apache.druid.guice.annotations.JSR311Resource;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

public class DruidGuiceContainer extends GuiceContainer
{
  @Nonnull
  public static Map<Scope, ComponentScope> populateScopeMap(Map<Scope, ComponentScope> map)
  {
    // Add the LazySingleton scope to the known scopes.
    map.put(DruidScopes.SINGLETON, ComponentScope.Singleton);
    return map;
  }

  private final Set<Class<?>> resources;
  private final Injector injector;

  private WebApplication webapp;

  @Inject
  public DruidGuiceContainer(
      Injector injector,
      @JSR311Resource Set<Class<?>> resources
  )
  {
    super(injector);
    this.injector = injector;
    this.resources = resources;
  }

  @Override
  protected ResourceConfig getDefaultResourceConfig(
      Map<String, Object> props, WebConfig webConfig
  )
  {
    return new DefaultResourceConfig(resources);
  }

  @Override
  protected void initiate(ResourceConfig config, WebApplication webapp)
  {
    this.webapp = webapp;
    // We need to initiate the webapp ourselves so that we can register the lazy singleton annotation with
    // the app such that it realizes that it is Singleton scoped.
    webapp.initiate(config, new ServletGuiceComponentProviderFactory(config, injector)
    {
      @Override
      public Map<Scope, ComponentScope> createScopeMap()
      {
        return populateScopeMap(super.createScopeMap());
      }
    });
  }

  @Override
  public WebApplication getWebApplication()
  {
    return webapp;
  }
}
