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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.util.Types;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.guava.DSuppliers;

/**
 */
public class JacksonConfigProvider<T> implements Provider<Supplier<T>>
{
  public static <T> void bind(Binder binder, String key, Class<T> clazz, T defaultVal)
  {
    binder.bind(Key.get(Types.newParameterizedType(Supplier.class, clazz)))
          .toProvider((Provider) of(key, clazz, defaultVal))
          .in(LazySingleton.class);
  }

  public static <T> JacksonConfigProvider<T> of(String key, Class<T> clazz)
  {
    return of(key, clazz, null);
  }

  public static <T> JacksonConfigProvider<T> of(String key, Class<T> clazz, T defaultVal)
  {
    return new JacksonConfigProvider<T>(key, clazz, null, defaultVal);
  }

  public static <T> JacksonConfigProvider<T> of(String key, TypeReference<T> clazz)
  {
    return of(key, clazz, null);
  }

  public static <T> JacksonConfigProvider<T> of(String key, TypeReference<T> typeRef, T defaultVal)
  {
    return new JacksonConfigProvider<T>(key, null, typeRef, defaultVal);
  }

  private final String key;
  private final Class<T> clazz;
  private final TypeReference<T> typeRef;
  private final T defaultVal;
  private JacksonConfigManager configManager;

  JacksonConfigProvider(String key, Class<T> clazz, TypeReference<T> typeRef, T defaultVal)
  {
    this.key = key;
    this.clazz = clazz;
    this.typeRef = typeRef;
    this.defaultVal = defaultVal;
  }

  @Inject
  public void configure(JacksonConfigManager configManager)
  {
    this.configManager = configManager;
  }

  @Override
  public Supplier<T> get()
  {
    if (clazz == null) {
      return DSuppliers.of(configManager.watch(key, typeRef, defaultVal));
    }
    else {
      return DSuppliers.of(configManager.watch(key, clazz, defaultVal));
    }
  }

}
