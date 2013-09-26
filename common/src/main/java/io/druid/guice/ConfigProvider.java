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

  private ConfigurationObjectFactory factory = null;

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
    this.factory = factory;
  }

  @Override
  public T get()
  {
    try {
      // ConfigMagic handles a null replacements
      Preconditions.checkNotNull(factory, "WTF!? Code misconfigured, inject() didn't get called.");
      return factory.buildWithReplacements(clazz, replacements);
    }
    catch (IllegalArgumentException e) {
      log.info("Unable to build instance of class[%s]", clazz);
      throw e;
    }
  }
}
