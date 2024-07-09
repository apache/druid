/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.common.semantic;

import org.apache.druid.error.DruidException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class SemanticUtils
{
  private static final Map<Class<?>, Map<Class<?>, Function<?, ?>>> OVERRIDES = new LinkedHashMap<>();

  /**
   * Allows the registration of overrides, which allows overriding of already existing mappings.
   * This allows extensions to register mappings.
   */
  @SuppressWarnings("unused")
  public static <C, T> void registerAsOverride(Class<C> clazz, Class<T> asInterface, Function<C, T> fn)
  {
    final Map<Class<?>, Function<?, ?>> classOverrides = OVERRIDES.computeIfAbsent(
        clazz,
        theClazz -> new LinkedHashMap<>()
    );

    final Function<?, ?> oldVal = classOverrides.get(asInterface);
    if (oldVal != null) {
      throw DruidException.defensive(
          "Attempt to side-override the same interface [%s] multiple times for the same class [%s].",
          asInterface,
          clazz
      );
    } else {
      classOverrides.put(asInterface, fn);
    }
  }

  public static <T> Map<Class<?>, Function<T, ?>> makeAsMap(Class<T> clazz)
  {
    final Map<Class<?>, Function<T, ?>> retVal = new HashMap<>();

    for (Method method : clazz.getMethods()) {
      if (method.isAnnotationPresent(SemanticCreator.class)) {
        if (method.getParameterCount() != 0) {
          throw DruidException.defensive("Method [%s] annotated with SemanticCreator was not 0-argument.", method);
        }

        retVal.put(method.getReturnType(), arg -> {
          try {
            return method.invoke(arg);
          }
          catch (InvocationTargetException | IllegalAccessException e) {
            throw DruidException.defensive().build(e, "Problem invoking method [%s]", method);
          }
        });
      }
    }

    final Map<Class<?>, Function<?, ?>> classOverrides = OVERRIDES.get(clazz);
    if (classOverrides != null) {
      for (Map.Entry<Class<?>, Function<?, ?>> overrideEntry : classOverrides.entrySet()) {
        //noinspection unchecked
        retVal.put(overrideEntry.getKey(), (Function<T, ?>) overrideEntry.getValue());
      }
    }

    return retVal;
  }
}
