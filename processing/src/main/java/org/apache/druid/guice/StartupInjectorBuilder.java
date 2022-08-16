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

import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.math.expr.ExpressionProcessingModule;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create the startup injector used to "prime" the modules for the
 * main injector.
 * <p>
 * Servers call the {@link #forServer()} method to configure server-style
 * properties and the server metrics. Servers must also add
 * {@code org.apache.druid.initialization.ExtensionsModule} which is
 * not visible here, and can't be added in the {@link #forServer()}
 * method.
 * <p>
 * Tests and clients must provide
 * properties via another mechanism.
 * <p>
 * If every test and client needs a module, it should be present here.
 */
public class StartupInjectorBuilder extends BaseInjectorBuilder<StartupInjectorBuilder>
{
  public StartupInjectorBuilder()
  {
    add(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new ConfigModule(),
        new NullHandlingModule(),
        new ExpressionProcessingModule(),
        binder -> binder.bind(DruidSecondaryModule.class)
    );
  }

  public StartupInjectorBuilder withProperties(Properties properties)
  {
    add(binder -> binder.bind(Properties.class).toInstance(properties));
    return this;
  }

  public StartupInjectorBuilder withEmptyProperties()
  {
    return withProperties(new Properties());
  }

  public StartupInjectorBuilder withExtensions()
  {
    add(new ExtensionsModule());
    return this;
  }

  public StartupInjectorBuilder forServer()
  {
    withExtensions();
    add(
        new PropertiesModule(Arrays.asList("common.runtime.properties", "runtime.properties")),
        new RuntimeInfoModule()
    );
    return this;
  }
}
