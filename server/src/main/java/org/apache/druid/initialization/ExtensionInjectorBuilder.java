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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.ExtensionsLoader;

/**
 * Injector builder which overrides service modules with extension
 * modules. Used only in the server, not in clients or tests.
 */
public class ExtensionInjectorBuilder extends DruidInjectorBuilder
{
  private final ServiceInjectorBuilder serviceBuilder;

  public ExtensionInjectorBuilder(ServiceInjectorBuilder serviceBuilder)
  {
    super(serviceBuilder);
    this.serviceBuilder = serviceBuilder;
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(baseInjector);
    for (DruidModule module : extnLoader.getFromExtensions(DruidModule.class)) {
      addModule(module);
    }
  }

  @Override
  public Injector build()
  {
    return Guice.createInjector(Modules.override(serviceBuilder.merge()).with(modules()));
  }
}
