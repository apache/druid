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

package org.apache.druid.java.util.common;

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

}
