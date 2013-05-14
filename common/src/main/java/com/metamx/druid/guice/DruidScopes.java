package com.metamx.druid.guice;

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
