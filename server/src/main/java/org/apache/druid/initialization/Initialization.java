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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Initialize Guice for a server. This is a legacy version, kept for
 * compatibility with existing tests. Clients and tests should use
 * the individual builders to create a non-server environment.
 * Clients (and tests) never load extensions, and so do not need
 * (and, in fact, should not use) the
 * {@link ExtensionInjectorBuilder}. Instead, simple tests can use
 * {@link org.apache.druid.guice.StartupInjectorBuilder
 * StartupInjectorBuilder} directly, passing in any needed modules.
 * <p>
 * Some tests use modules that rely on the "startup injector" to
 * inject values into a module. In that case, tests should use two
 * builders: the {@code StartupInjectorBuilder} followed by
 * the {@link CoreInjectorBuilder} class to hold extra modules.
 * Look for references to {@link CoreInjectorBuilder} to find examples
 * of this pattern.
 * <p>
 * In both cases, the injector builders have options to add the full
 * set of server modules. Tests should not load those modules. Instead,
 * let the injector builders provide just the required set, and then
 * explicitly list the (small subset) of modules needed by any given test.
 * <p>
 * The server initialization formerly done here is now done in
 * {@link org.apache.druid.cli.GuiceRunnable GuiceRunnable} by way of
 * the {@link ServerInjectorBuilder}.
 */
public class Initialization
{
  // Use individual builders for testing: this method brings in
  // server-only dependencies, which is generally not desired.
  // See class comment for more information.
  @Deprecated
  public static Injector makeInjectorWithModules(
      final Injector baseInjector,
      final Iterable<? extends Module> modules
  )
  {
    return ServerInjectorBuilder.makeServerInjector(baseInjector, ImmutableSet.of(), modules);
  }
}
