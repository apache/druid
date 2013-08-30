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

package io.druid.guice.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A Module to add lifecycle management to the injector.  {@link DruidGuiceExtensions} must also be included.
 */
public class LifecycleModule implements Module
{
  private final LifecycleScope scope = new LifecycleScope(Lifecycle.Stage.NORMAL);
  private final LifecycleScope lastScope = new LifecycleScope(Lifecycle.Stage.LAST);
  private final List<Key<?>> eagerClasses = new CopyOnWriteArrayList<Key<?>>();
  public boolean configured = false;

  /**
   * Registers a class to instantiate eagerly.  Classes mentioned here will be pulled out of
   * the injector with an injector.getInstance() call when the lifecycle is created.
   *
   * Eagerly loaded classes will *not* be automatically added to the Lifecycle unless they are bound to the proper
   * scope.  That is, they are generally eagerly loaded because the loading operation will produce some beneficial
   * side-effect even if nothing actually directly depends on the instance.
   *
   * This mechanism exists to allow the {@link Lifecycle} to be the primary entry point from the injector, not to
   * auto-register things with the {@link Lifecycle}.  It is also possible to just bind things eagerly with Guice,
   * it is not clear which is actually the best approach.  This is more explicit, but eager bindings inside of modules
   * is less error-prone.
   *
   * @param clazz, the class to instantiate
   * @return this, for chaining.
   */
  public LifecycleModule register(Class<?> clazz)
  {
    return registerKey(Key.get(clazz));
  }

  /**
   * Registers a class/annotation combination to instantiate eagerly.  Classes mentioned here will be pulled out of
   * the injector with an injector.getInstance() call when the lifecycle is created.
   *
   * Eagerly loaded classes will *not* be automatically added to the Lifecycle unless they are bound to the proper
   * scope.  That is, they are generally eagerly loaded because the loading operation will produce some beneficial
   * side-effect even if nothing actually directly depends on the instance.
   *
   * This mechanism exists to allow the {@link Lifecycle} to be the primary entry point from the injector, not to
   * auto-register things with the {@link Lifecycle}.  It is also possible to just bind things eagerly with Guice,
   * it is not clear which is actually the best approach.  This is more explicit, but eager bindings inside of modules
   * is less error-prone.
   *
   * @param clazz, the class to instantiate
   * @param annotation The annotation instance to register with Guice, usually a Named annotation
   * @return this, for chaining.
   */
  public LifecycleModule register(Class<?> clazz, Annotation annotation)
  {
    return registerKey(Key.get(clazz, annotation));
  }

  /**
   * Registers a class/annotation combination to instantiate eagerly.  Classes mentioned here will be pulled out of
   * the injector with an injector.getInstance() call when the lifecycle is created.
   *
   * Eagerly loaded classes will *not* be automatically added to the Lifecycle unless they are bound to the proper
   * scope.  That is, they are generally eagerly loaded because the loading operation will produce some beneficial
   * side-effect even if nothing actually directly depends on the instance.
   *
   * This mechanism exists to allow the {@link Lifecycle} to be the primary entry point from the injector, not to
   * auto-register things with the {@link Lifecycle}.  It is also possible to just bind things eagerly with Guice,
   * it is not clear which is actually the best approach.  This is more explicit, but eager bindings inside of modules
   * is less error-prone.
   *
   * @param clazz, the class to instantiate
   * @param annotation The annotation class to register with Guice
   * @return this, for chaining
   */
  public LifecycleModule register(Class<?> clazz, Class<? extends Annotation> annotation)
  {
    return registerKey(Key.get(clazz, annotation));
  }

  /**
   * Registers a key to instantiate eagerly.  {@link Key}s mentioned here will be pulled out of
   * the injector with an injector.getInstance() call when the lifecycle is created.
   *
   * Eagerly loaded classes will *not* be automatically added to the Lifecycle unless they are bound to the proper
   * scope.  That is, they are generally eagerly loaded because the loading operation will produce some beneficial
   * side-effect even if nothing actually directly depends on the instance.
   *
   * This mechanism exists to allow the {@link Lifecycle} to be the primary entry point from the injector, not to
   * auto-register things with the {@link Lifecycle}.  It is also possible to just bind things eagerly with Guice,
   * it is not clear which is actually the best approach.  This is more explicit, but eager bindings inside of modules
   * is less error-prone.
   *
   * @param key The key to use in finding the DruidNode instance
   * @return this, for chaining
   */
  public LifecycleModule registerKey(Key<?> key)
  {
    synchronized (eagerClasses) {
      Preconditions.checkState(!configured, "Cannot register key[%s] after configuration.", key);
    }
    eagerClasses.add(key);
    return this;
  }

  @Override
  public void configure(Binder binder)
  {
    synchronized (eagerClasses) {
      configured = true;
      binder.bindScope(ManageLifecycle.class, scope);
      binder.bindScope(ManageLifecycleLast.class, lastScope);
    }
  }

  @Provides @LazySingleton
  public Lifecycle getLifecycle(final Injector injector)
  {
    Lifecycle lifecycle = new Lifecycle(){
      @Override
      public void start() throws Exception
      {
        for (Key<?> key : eagerClasses) {
          injector.getInstance(key); // Pull the key so as to "eagerly" load up the class.
        }
        super.start();
      }
    };
    scope.setLifecycle(lifecycle);
    lastScope.setLifecycle(lifecycle);

    return lifecycle;
  }
}
