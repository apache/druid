/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.common.collect.Lists;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

import java.util.List;

/**
 * A scope that adds objects to the Lifecycle.  This is by definition also a lazy singleton scope.
 */
public class LifecycleScope implements Scope
{
  private static final Logger log = new Logger(LifecycleScope.class);
  private final Lifecycle.Stage stage;

  private Lifecycle lifecycle;
  private List<Object> instances = Lists.newLinkedList();

  public LifecycleScope(Lifecycle.Stage stage)
  {
    this.stage = stage;
  }

  public void setLifecycle(Lifecycle lifecycle)
  {
    this.lifecycle = lifecycle;
    synchronized (instances) {
      for (Object instance : instances) {
        lifecycle.addManagedInstance(instance);
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
            }
            else {
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
