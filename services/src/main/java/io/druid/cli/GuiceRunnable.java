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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.initialization.Initialization;
import io.druid.initialization.LogLevelAdjuster;

import java.util.List;

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
      LogLevelAdjuster.register();
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

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
