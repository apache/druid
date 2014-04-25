/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.server;

import java.lang.reflect.InvocationTargetException;

public class VMUtils
{
  public static long getMaxDirectMemory() throws UnsupportedOperationException
  {
    try {
      Class<?> vmClass = Class.forName("sun.misc.VM");
      Object maxDirectMemoryObj = vmClass.getMethod("maxDirectMemory").invoke(null);

      if (maxDirectMemoryObj == null || !(maxDirectMemoryObj instanceof Number)) {
        throw new UnsupportedOperationException(String.format("Cannot determine maxDirectMemory from [%s]", maxDirectMemoryObj));
      } else {
        return ((Number) maxDirectMemoryObj).longValue();
      }
    }
    catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException("No VM class, cannot do memory check.", e);
    }
    catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException("VM.maxDirectMemory doesn't exist, cannot do memory check.", e);
    }
    catch (InvocationTargetException e) {
      throw new RuntimeException("static method shouldn't throw this", e);
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException("public method, shouldn't throw this", e);
    }
  }
}
