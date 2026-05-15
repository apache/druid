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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;

import java.util.Set;

/**
 * Initialize Guice for a server. Clients and tests should use
 * the individual builders to create a non-server environment.
 * <p>
 * This class is in this package for historical reasons. The proper
 * place is in the same module as {@code GuiceRunnable} since this
 * class should only ever be used by servers. It is here until
 * tests are converted to use the builders, and @{link Initialization}
 * is deleted.
 */
public class ServerInjectorBuilder
{
  private final Injector baseInjector;
  private Set<NodeRole> nodeRoles;
  private Iterable<? extends Module> modules;

  /**
   * Create a server injector. Located here for testing. Should only be
   * used by {@code GuiceRunnable} (and tests).
   *
   * @param nodeRoles the roles which this server provides
   * @param baseInjector the startup injector
   * @param modules modules for this server
   * @return the injector for the server
   */
  @VisibleForTesting
  public static Injector makeServerInjector(
      final Injector baseInjector,
      final Set<NodeRole> nodeRoles,
      final Iterable<? extends Module> modules
  )
  {
    return new ServerInjectorBuilder(baseInjector)
        .nodeRoles(nodeRoles)
        .serviceModules(modules)
        .build();
  }

  public ServerInjectorBuilder(Injector baseInjector)
  {
    this.baseInjector = baseInjector;
  }

  public ServerInjectorBuilder nodeRoles(final Set<NodeRole> nodeRoles)
  {
    this.nodeRoles = nodeRoles == null ? ImmutableSet.of() : nodeRoles;
    return this;
  }

  public ServerInjectorBuilder serviceModules(final Iterable<? extends Module> modules)
  {
    this.modules = modules;
    return this;
  }

  public Injector build()
  {
    Preconditions.checkNotNull(baseInjector);
    Preconditions.checkNotNull(nodeRoles);

    Module registerNodeRoleModule = registerNodeRoleModule(nodeRoles);

    // Child injector, with the registered node roles
    Injector childInjector = baseInjector.createChildInjector(registerNodeRoleModule);

    // Create the core set of modules shared by all services.
    // Here and below, the modules are filtered by the load modules list and
    // the set of roles which this server provides.
    CoreInjectorBuilder coreBuilder = new CoreInjectorBuilder(childInjector, nodeRoles).forServer();

    // Override with the per-service modules.
    ServiceInjectorBuilder serviceBuilder = (ServiceInjectorBuilder) new ServiceInjectorBuilder(coreBuilder).addAll(
        Iterables.concat(
            // bind nodeRoles for the new injector as well
            ImmutableList.of(registerNodeRoleModule),
            modules
        )
    );

    // Override again with extensions.
    return new ExtensionInjectorBuilder(serviceBuilder).build();
  }

  public static Module registerNodeRoleModule(Set<NodeRole> nodeRoles)
  {
    return binder -> {
      Multibinder<NodeRole> selfBinder = Multibinder.newSetBinder(binder, NodeRole.class, Self.class);
      nodeRoles.forEach(nodeRole -> selfBinder.addBinding().toInstance(nodeRole));

      MapBinder.newMapBinder(
          binder,
          new TypeLiteral<NodeRole>(){},
          new TypeLiteral<Set<Class<? extends DruidService>>>(){}
      );
    };
  }
}
