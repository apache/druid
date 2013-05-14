package com.metamx.druid.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.skife.config.ConfigurationObjectFactory;

/**
 */
public class ConfigProvider<T> implements Provider<T>
{
  public static <T> void bind(Binder binder, Class<T> clazz)
  {
    binder.bind(clazz).toProvider(of(clazz)).in(DruidScopes.SINGLETON);
  }

  public static <T> Provider<T> of(Class<T> clazz)
  {
    return new ConfigProvider<T>(clazz);
  }

  private final Class<T> clazz;

  private T object = null;

  public ConfigProvider(
      Class<T> clazz
  )
  {
    this.clazz = clazz;
  }

  @Inject
  public void inject(ConfigurationObjectFactory factory)
  {
    object = factory.build(clazz);
  }

  @Override
  public T get()
  {
    return Preconditions.checkNotNull(object, "WTF!? Code misconfigured, inject() didn't get called.");
  }
}
