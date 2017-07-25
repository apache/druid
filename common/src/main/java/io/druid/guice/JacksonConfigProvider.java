/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    } else {
      return DSuppliers.of(configManager.watch(key, clazz, defaultVal));
    }
  }

}
