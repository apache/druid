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

package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;

/**
 * A Module to add lifecycle management to the injector.  {@link DruidGuiceExtensions} must also be included.
 */
public class LifecycleModule implements Module
{
  private final LifecycleScope scope = new LifecycleScope();
  private final Key<?>[] eagerClasses;

  /**
   * A constructor that takes a list of classes to instantiate eagerly.  Class {@link Key}s mentioned here will
   * be pulled out of the injector with an injector.getInstance() call when the lifecycle is created.
   *
   * Eagerly loaded classes will *not* be automatically added to the Lifecycle unless they are bound to the proper
   * scope.
   *
   * This mechanism exists to allow the {@link Lifecycle} to be the primary entry point from the injector, not to
   * auto-register things with the {@link Lifecycle}
   *
   * @param eagerClasses set of classes to instantiate eagerly
   */
  public LifecycleModule(
      Key<?>... eagerClasses
  )
  {
    this.eagerClasses = eagerClasses;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bindScope(ManageLifecycle.class, scope);
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

    return lifecycle;
  }
}
