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

package io.druid.java.util.common;

import com.google.common.base.Function;

import java.util.List;
import java.util.Map;

/**
 */
public class MapUtils
{
  public static String getString(Map<String, Object> in, String key)
  {
    return getString(in, key, null);
  }

  public static String getString(Map<String, Object> in, String key, String defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    return retVal.toString();
  }

  public static Function<Map<String, Object>, String> stringFromMapFn(final String key)
  {
    return new Function<Map<String, Object>, String>()
    {
      @Override
      public String apply(Map<String, Object> map)
      {
        return MapUtils.getString(map, key);
      }
    };
  }

  public static <RetVal> RetVal lookupStringValInMap(Map<String, Object> map, String key, Map<String, RetVal> lookupMap)
  {
    String lookupKey = getString(map, key);
    RetVal retVal = lookupMap.get(lookupKey);

    if (retVal == null) {
      throw new IAE("Unknown %s[%s], known values are%s", key, lookupKey, lookupMap.keySet());
    }

    return retVal;
  }

  public static int getInt(Map<String, Object> in, String key)
  {
    return getInt(in, key, null);
  }

  public static int getInt(Map<String, Object> in, String key, Integer defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    try {
      return Integer.parseInt(retVal.toString());
    }
    catch (NumberFormatException e) {
      throw new IAE(e, "Key[%s] should be an int, was[%s]", key, retVal);
    }
  }

  public static long getLong(Map<String, Object> in, String key)
  {
    return getLong(in, key, null);
  }

  public static long getLong(Map<String, Object> in, String key, Long defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    try {
      return Long.parseLong(retVal.toString());
    }
    catch (NumberFormatException e) {
      throw new IAE(e, "Key[%s] should be a long, was[%s]", key, retVal);
    }
  }

  public static double getDouble(Map<String, Object> in, String key)
  {
    return getDouble(in, key, null);
  }

  public static double getDouble(Map<String, Object> in, String key, Double defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    try {
      return Double.parseDouble(retVal.toString());
    }
    catch (NumberFormatException e) {
      throw new IAE(e, "Key[%s] should be a double, was[%s]", key, retVal);
    }
  }

  public static List<Object> getList(Map<String, Object> in, String key)
  {
    return getList(in, key, null);
  }

  public static List<Object> getList(Map<String, Object> in, String key, List<Object> defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    try {
      return (List<Object>) retVal;
    }
    catch (ClassCastException e) {
      throw new IAE("Key[%s] should be a list, was [%s]", key, retVal);
    }
  }

  public static Map<String, Object> getMap(Map<String, Object> in, String key)
  {
    return getMap(in, key, null);
  }

  public static Map<String, Object> getMap(Map<String, Object> in, String key, Map<String, Object> defaultValue)
  {
    Object retVal = in.get(key);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new IAE("Key[%s] is required in map[%s]", key, in);
      }

      return defaultValue;
    }

    try {
      return (Map<String, Object>) retVal;
    }
    catch (ClassCastException e) {
      throw new IAE("Key[%s] should be a map, was [%s]", key, retVal);
    }
  }
}
