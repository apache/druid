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

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.metamx.common.lifecycle.Lifecycle;

/**
 */
public class DruidScopes
{
  public static final Scope SINGLETON = new Scope()
  {
    @Override
    public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped)
    {
      return Scopes.SINGLETON.scope(key, unscoped);
    }

    @Override
    public String toString()
    {
      return "DruidScopes.SINGLETON";
    }
  };

  public static final Scope LIFECYCLE = new Scope()
  {
    @Override
    public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped)
    {
      return new Provider<T>()
      {

        private Provider<T> provider;

        @Inject
        public void inject(final Lifecycle lifecycle)
        {
          provider = Scopes.SINGLETON.scope(
              key,
              new Provider<T>()
              {

                @Override
                public T get()
                {
                  return lifecycle.addManagedInstance(unscoped.get());
                }
              }
          );
        }

        @Override
        public T get()
        {
          System.out.println(provider);
          return provider.get();
        }
      };
    }

    @Override
    public String toString()
    {
      return "DruidScopes.LIFECYCLE";
    }
  };
}
