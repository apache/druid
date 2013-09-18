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

package io.druid.common.utils;

import com.metamx.common.ISE;

import java.util.Properties;

/**
 */
public class PropUtils
{
  public static String getProperty(Properties props, String property)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      throw new ISE("Property[%s] not specified.", property);
    }

    return retVal;
  }

  public static int getPropertyAsInt(Properties props, String property)
  {
    return getPropertyAsInt(props, property, null);
  }

  public static int getPropertyAsInt(Properties props, String property, Integer defaultValue)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new ISE("Property[%s] not specified.", property);
      }
      else {
        return defaultValue;
      }
    }

    try {
      return Integer.parseInt(retVal);
    }
    catch (NumberFormatException e) {
      throw new ISE(e, "Property[%s] is expected to be an int, it is not[%s].", retVal);
    }
  }
}
