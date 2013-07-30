package com.metamx.druid.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.logger.Logger;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Map;

/**
 */
public class ConfigProvider<T> implements Provider<T>
{
  private static final Logger log = new Logger(ConfigProvider.class);

  public static <T> void bind(Binder binder, Class<T> clazz)
  {
    binder.bind(clazz).toProvider(of(clazz)).in(LazySingleton.class);
  }

  public static <T> void bind(Binder binder, Class<T> clazz, Map<String, String> replacements)
  {
    binder.bind(clazz).toProvider(of(clazz, replacements)).in(LazySingleton.class);
  }

  public static <T> Provider<T> of(Class<T> clazz)
  {
    return of(clazz, null);
  }

  public static <T> Provider<T> of(Class<T> clazz, Map<String, String> replacements)
  {
    return new ConfigProvider<T>(clazz, replacements);
  }

  private final Class<T> clazz;
  private final Map<String, String> replacements;

  private T object = null;

  public ConfigProvider(
      Class<T> clazz,
      Map<String, String> replacements
  )
  {
    this.clazz = clazz;
    this.replacements = replacements;
  }

  @Inject
  public void inject(ConfigurationObjectFactory factory)
  {
    try {
      // ConfigMagic handles a null replacements
      object = factory.buildWithReplacements(clazz, replacements);
    }
    catch (IllegalArgumentException e) {
      log.info("Unable to build instance of class[%s]", clazz);
      throw e;
    }
  }

  @Override
  public T get()
  {
    return Preconditions.checkNotNull(object, "WTF!? Code misconfigured, inject() didn't get called.");
  }
}
