package com.metamx.druid.guice;

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

  private Lifecycle lifecycle;
  private List<Object> instances = Lists.newLinkedList();

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
                lifecycle.addMaybeStartManagedInstance(retVal);
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
