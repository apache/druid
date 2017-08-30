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

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;

import io.druid.initialization.Initialization;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.log.StartupLoggingConfig;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
public abstract class GuiceRunnable implements Runnable
{
  private final Logger log;

  private Injector baseInjector;

  public GuiceRunnable(Logger log)
  {
    this.log = log;
  }

  @Inject
  public void configure(Injector injector)
  {
    this.baseInjector = injector;
  }

  protected abstract List<? extends Module> getModules();

  public Injector makeInjector()
  {
    try {
      return Initialization.makeInjectorWithModules(
          baseInjector, getModules()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Lifecycle initLifecycle(Injector injector)
  {
    try {
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
      final StartupLoggingConfig startupLoggingConfig = injector.getInstance(StartupLoggingConfig.class);

      log.info(
          "Starting up with processors[%,d], memory[%,d], maxMemory[%,d].",
          Runtime.getRuntime().availableProcessors(),
          Runtime.getRuntime().totalMemory(),
          Runtime.getRuntime().maxMemory()
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
      throw Throwables.propagate(e);
    }
  }
}
