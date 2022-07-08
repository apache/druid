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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.utils.JvmUtils;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
public abstract class GuiceRunnable implements Runnable
{
  private final Logger log;

  private Properties properties;
  private Injector baseInjector;

  public GuiceRunnable(Logger log)
  {
    this.log = log;
  }

  /**
   * This method overrides {@link Runnable} just in order to be able to define it as "entry point" for
   * "Unused declarations" inspection in IntelliJ.
   */
  @Override
  public abstract void run();

  @Inject
  public void configure(Properties properties, Injector injector)
  {
    this.properties = properties;
    this.baseInjector = injector;
  }

  protected Properties getProperties()
  {
    return properties;
  }

  protected abstract List<? extends Module> getModules();

  public Injector makeInjector()
  {
    // Pass an empty set of nodeRoles for non-ServerRunnables.
    // They will still load all modules except for the ones annotated with `LoadOn`.
    return makeInjector(ImmutableSet.of());
  }

  public Injector makeInjector(Set<NodeRole> nodeRoles)
  {
    Module registerNodeRoleModule = registerNodeRoleModule(nodeRoles);
    try {
      return Initialization.makeInjectorWithModules(
          nodeRoles,
          baseInjector.createChildInjector(registerNodeRoleModule),
          Iterables.concat(
              // bind nodeRoles for the new injector as well
              ImmutableList.of(registerNodeRoleModule),
              getModules()
          )
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Module registerNodeRoleModule(Set<NodeRole> nodeRoles)
  {
    if (nodeRoles.isEmpty()) {
      return binder -> {};
    } else {
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

  public Lifecycle initLifecycle(Injector injector)
  {
    return initLifecycle(injector, log);
  }

  public static Lifecycle initLifecycle(Injector injector, Logger log)
  {
    try {
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
      final StartupLoggingConfig startupLoggingConfig = injector.getInstance(StartupLoggingConfig.class);

      Long directSizeBytes = null;
      try {
        directSizeBytes = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
      }
      catch (UnsupportedOperationException ignore) {
        // querying direct memory is not supported
      }

      log.info(
          "Starting up with processors[%,d], memory[%,d], maxMemory[%,d]%s. Properties follow.",
          JvmUtils.getRuntimeInfo().getAvailableProcessors(),
          JvmUtils.getRuntimeInfo().getTotalHeapSizeBytes(),
          JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
          directSizeBytes != null ? StringUtils.format(", directMemory[%,d]", directSizeBytes) : ""
      );

      if (startupLoggingConfig.isLogProperties()) {
        final Set<String> maskProperties = Sets.newHashSet(startupLoggingConfig.getMaskProperties());
        final Properties props = injector.getInstance(Properties.class);

        for (String propertyName : Ordering.natural().sortedCopy(props.stringPropertyNames())) {
          String property = props.getProperty(propertyName);
          for (String masked : maskProperties) {
            if (propertyName.contains(masked)) {
              property = "<masked>";
              break;
            }
          }
          log.info("* %s: %s", propertyName, property);
        }
      }

      try {
        lifecycle.start();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }

      return lifecycle;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
