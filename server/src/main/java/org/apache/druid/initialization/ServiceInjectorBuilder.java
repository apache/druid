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
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.druid.guice.DruidInjectorBuilder;

/**
 * Injector builder for a service within a server. In the server, this builder
 * is input to the {@link ExtensionInjectorBuilder}. Also used to build clients
 * or tests, without extensions, where this builder itself builds the injector.
 */
public class ServiceInjectorBuilder extends DruidInjectorBuilder
{
  private final CoreInjectorBuilder coreBuilder;

  public ServiceInjectorBuilder(
      final CoreInjectorBuilder coreBuilder
  )
  {
    super(coreBuilder);
    this.coreBuilder = coreBuilder;
  }

  public Module merge()
  {
    return Modules.override(coreBuilder.modules()).with(modules());
  }

  @Override
  public Injector build()
  {
    return Guice.createInjector(merge());
  }

}
