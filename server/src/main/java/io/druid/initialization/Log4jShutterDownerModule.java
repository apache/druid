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

package io.druid.initialization;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;

import io.druid.common.config.Log4jShutdown;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.ShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;

public class Log4jShutterDownerModule implements Module
{
  private static final Logger log = new Logger(Log4jShutterDownerModule.class);

  @Override
  public void configure(Binder binder)
  {
    // Instantiate eagerly so that we get everything registered and put into the Lifecycle
    // This makes the shutdown run pretty darn near last.

    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if(loader == null) {
        loader = getClass().getClassLoader();
      }
      // Reflection to try and allow non Log4j2 stuff to run. This acts as a gateway to stop errors in the next few lines
      // In log4j api
      final Class<?> logManagerClazz = Class.forName("org.apache.logging.log4j.LogManager", false, loader);
      // In log4j core
      final Class<?> callbackRegistryClazz = Class.forName("org.apache.logging.log4j.core.util.ShutdownCallbackRegistry", false, loader);

      final LoggerContextFactory contextFactory = LogManager.getFactory();
      if (!(contextFactory instanceof Log4jContextFactory)) {
        log.warn(
            "Expected [%s] found [%s]. Unknown class for context factory. Not logging shutdown",
            Log4jContextFactory.class.getCanonicalName(),
            contextFactory.getClass().getCanonicalName()
        );
        return;
      }
      final ShutdownCallbackRegistry registry = ((Log4jContextFactory) contextFactory).getShutdownCallbackRegistry();
      if (!(registry instanceof Log4jShutdown)) {
        log.warn(
            "Shutdown callback registry expected class [%s] found [%s]. Skipping shutdown registry",
            Log4jShutdown.class.getCanonicalName(),
            registry.getClass().getCanonicalName()
        );
        return;
      }
      binder.bind(Log4jShutdown.class).toInstance((Log4jShutdown) registry);
      binder.bind(Key.get(Log4jShutterDowner.class, Names.named("ForTheEagerness")))
            .to(Log4jShutterDowner.class)
            .asEagerSingleton();
    }
    catch (ClassNotFoundException | ClassCastException | LinkageError e) {
      log.warn(e, "Not registering log4j shutdown hooks. Not using log4j?");
    }
  }


  @ManageLifecycle
  @Provides
  public Log4jShutterDowner getShutterDowner(
      Log4jShutdown log4jShutdown
  )
  {
    return new Log4jShutterDowner(log4jShutdown);
  }

  public static class Log4jShutterDowner
  {
    private final Log4jShutdown log4jShutdown;

    public Log4jShutterDowner(Log4jShutdown log4jShutdown)
    {
      this.log4jShutdown = log4jShutdown;
    }

    @LifecycleStart
    public void start()
    {
      log.debug("Log4j shutter downer is waiting");
    }

    @LifecycleStop
    public void stop()
    {
      if (log4jShutdown != null) {
        log.debug("Shutting down log4j");
        log4jShutdown.stop();
      } else {
        log.warn("Log4j shutdown was registered in lifecycle but no shutdown object exists!");
      }
    }
  }
}
