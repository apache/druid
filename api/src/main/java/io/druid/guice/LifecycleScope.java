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

package io.druid.guice;

import com.google.common.collect.Lists;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;

import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;

import java.util.List;

/**
 * A scope that adds objects to the Lifecycle.  This is by definition also a lazy singleton scope.
 */
public class LifecycleScope implements Scope
{
  private static final Logger log = new Logger(LifecycleScope.class);
  private final Lifecycle.Stage stage;

  private Lifecycle lifecycle;
  private final List<Object> instances = Lists.newLinkedList();

  public LifecycleScope(Lifecycle.Stage stage)
  {
    this.stage = stage;
  }

  public void setLifecycle(Lifecycle lifecycle)
  {
    synchronized (instances) {
      this.lifecycle = lifecycle;
      for (Object instance : instances) {
        lifecycle.addManagedInstance(instance, stage);
      }
    }
  }

  @Override
  public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped)
  {
    return new Provider<T>()
    {
      private volatile T value = null;

      @Override
      public synchronized T get()
      {
        if (value == null) {
          final T retVal = unscoped.get();

          synchronized (instances) {
            if (lifecycle == null) {
              instances.add(retVal);
            } else {
              try {
                lifecycle.addMaybeStartManagedInstance(retVal, stage);
              }
              catch (Exception e) {
                log.warn(e, "Caught exception when trying to create a[%s]", key);
                return null;
              }
            }
          }

          value = retVal;
        }

        return value;
      }
    };
  }
}
