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

package io.druid.query;

import com.google.common.collect.Maps;
import io.druid.java.util.common.logger.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@Deprecated
public class ReflectionLoaderThingy<T>
{
  private static final Logger log = new Logger(ReflectionLoaderThingy.class);

  public static <K> ReflectionLoaderThingy<K> create(Class<K> interfaceClass)
  {
    return new ReflectionLoaderThingy<K>(interfaceClass);
  }

  Map<Class<?>, AtomicReference<T>> toolChestMap = Maps.newConcurrentMap();

  private final Class<T> interfaceClass;

  public ReflectionLoaderThingy(
      Class<T> interfaceClass
  )
  {
    this.interfaceClass = interfaceClass;
  }

  public T getForObject(Object keyObject)
  {
    Class<?> clazz = keyObject.getClass();

    AtomicReference<T> retVal = toolChestMap.get(clazz);

    if (retVal == null) {
      String interfaceName = interfaceClass.getSimpleName();

      AtomicReference<T> retVal1;
      try {
        final Class<?> queryToolChestClass = Class.forName(String.format("%s%s", clazz.getName(), interfaceName));
        retVal1 = new AtomicReference<T>(interfaceClass.cast(queryToolChestClass.newInstance()));
      }
      catch (Exception e) {
        log.warn(e, "Unable to load interface[%s] for input class[%s]", interfaceClass, clazz);
        retVal1 = new AtomicReference<T>(null);
      }
      retVal = retVal1;

      toolChestMap.put(clazz, retVal);
    }

    return retVal.get();
  }
}
