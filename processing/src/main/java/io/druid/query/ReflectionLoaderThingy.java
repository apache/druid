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

package io.druid.query;

import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;

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
