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
import com.google.inject.Module;

import javax.inject.Inject;

/**
 * Module for the extensions loader. Add to the startup injector
 * for Druid servers. Not visible to the {@link StartupInjectorBuilder},
 * so must be added by servers explicitly.
 */
public class ExtensionsModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ExtensionsLoader.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
    JsonConfigProvider.bind(binder, "druid.modules", ModulesConfig.class);
  }

  /**
   * Transfers the now-populated extension loader instance from the
   * startup to the main injector. Not done in {@code DruidSecondaryModule}
   * because extensions are loaded only in the server, but
   * {@code DruidSecondaryModule} is used for tests and clients also.
   */
  public static class SecondaryModule implements Module
  {
    private final ExtensionsLoader extnLoader;

    @Inject
    public SecondaryModule(final ExtensionsLoader extnLoader)
    {
      this.extnLoader = extnLoader;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(ExtensionsLoader.class).toInstance(extnLoader);
    }
  }
}
