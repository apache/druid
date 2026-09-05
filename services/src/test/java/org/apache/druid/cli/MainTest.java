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

package org.apache.druid.cli;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.server.system.handler.SystemTableQueryResource;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class MainTest
{
  private static Stream<ServerRunnable> runnables()
  {
    return Stream.of(
        new CliOverlord(),
        new CliBroker(),
        new CliHistorical(),
        new CliCoordinator(),
        new CliMiddleManager(),
        new CliRouter(),
        new CliIndexer()
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("runnables")
  public void testSimpleInjection(ServerRunnable runnable)
  {
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assertions.assertNotNull(runnable.makeInjector(runnable.getNodeRoles(new Properties())));
  }

  @Test
  public void testMiddleManagerNativeServerPropertiesInjection()
  {
    final CliMiddleManager middleManager = new CliMiddleManager();
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(middleManager);

    final Injector middleManagerInjector = middleManager.makeInjector(
        middleManager.getNodeRoles(new Properties())
    );

    // Middle Managers expose /druid/v2 through the resource restricted to internal system-table scans.
    Assertions.assertNotNull(middleManagerInjector.getInstance(SystemTableQueryResource.class));
    Assertions.assertTrue(
        systemTableDataProviders(middleManagerInjector).containsKey(ServerPropertiesTableDescriptor.TABLE_NAME)
    );
  }

  private static Map<String, SystemTableDataProvider> systemTableDataProviders(final Injector injector)
  {
    return injector.getInstance(Key.get(new TypeLiteral<>() {}));
  }
}
