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

package org.apache.druid.indexing.overlord.autoscaling.gce;

import org.apache.druid.java.util.common.StringUtils;

import java.util.Iterator;
import java.util.List;

/**
 * Simple collection of utilities extracted to ease testing and simplify the GceAutoScaler class
 */
public class GceUtils
{

  /**
   * converts https://www.googleapis.com/compute/v1/projects/X/zones/Y/instances/name-of-the-thing
   * into just `name-of-the-thing` as it is needed by the other pieces of the API
   */
  public static String extractNameFromInstance(String instance)
  {
    String name = instance;
    if (instance != null && !instance.isEmpty()) {
      int lastSlash = instance.lastIndexOf('/');
      if (lastSlash > -1) {
        name = instance.substring(lastSlash + 1);
      } else {
        name = instance; // let's assume not the URI like thing
      }
    }
    return name;
  }

  /**
   * Converts a list of terms to a 'OR' list of terms to look for a specific 'key'
   */
  public static String buildFilter(List<String> list, String key)
  {
    if (list == null || list.isEmpty() || key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Arguments cannot be empty of null");
    }
    Iterator<String> it = list.iterator();

    StringBuilder sb = new StringBuilder();
    sb.append(StringUtils.format("(%s = \"%s\")", key, it.next()));
    while (it.hasNext()) {
      sb.append(" OR ").append(StringUtils.format("(%s = \"%s\")", key, it.next()));
    }
    return sb.toString();
  }

  // cannot build it!
  private GceUtils()
  {
  }
}
