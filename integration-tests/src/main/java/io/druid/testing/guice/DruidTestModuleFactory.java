/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
