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

package org.apache.druid.quidem;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * Utility class to provide interface implementation based on a map of string
 * values.
 *
 * intended usage: MapToInterfaceHandler.newInstanceFor(TargetInterface.class,
 * map);
 */
class MapToInterfaceHandler implements InvocationHandler
{
  private Map<String, String> backingMap;

  @SuppressWarnings("unchecked")
  public static <T> T newInstanceFor(Class<T> clazz, Map<String, String> queryParams)
  {
    return (T) Proxy.newProxyInstance(
        clazz.getClassLoader(),
        new Class[] {clazz},
        new MapToInterfaceHandler(queryParams)
    );
  }

  private MapToInterfaceHandler(Map<String, String> backingMap)
  {
    this.backingMap = backingMap;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
  {
    Class<?> returnType = method.getReturnType();
    String obj = backingMap.get(method.getName());
    if (obj == null) {
      return method.getDefaultValue();
    } else {
      if (returnType.isInstance(obj)) {
        return obj;
      }
      return uglyCastCrap(obj, returnType);
    }
  }

  private Object uglyCastCrap(String obj, Class<?> returnType)
  {
    if (returnType == int.class) {
      return Integer.parseInt(obj);
    }
    throw new RuntimeException("don't know how to handle conversion to " + returnType);
  }
}
