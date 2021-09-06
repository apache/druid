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

package org.apache.druid.sql.avatica;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Properties;

public class DruidURLContext
{
  private static final ThreadLocal<Properties> PROPERTIES_THREAD_LOCAL = ThreadLocal.withInitial(() -> null);
  private static final Logger LOG = new Logger(DruidURLContext.class);

  @Nullable
  public static Properties getCurrentContext()
  {
    return PROPERTIES_THREAD_LOCAL.get();
  }

  public static void populateContext(Map<String, String[]> urlParams)
  {
    PROPERTIES_THREAD_LOCAL.set(getProperties(urlParams));
  }

  @Nullable
  private static Properties getProperties(Map<String, String[]> urlParams)
  {
    try {
      if (null == urlParams) {
        return null;
      }

      Properties urlContext = new Properties();
      for (Map.Entry<String, String[]> entry : urlParams.entrySet()) {
        for (String value : entry.getValue()) {
          if (Strings.isNullOrEmpty(value)) {
            continue;
          }
          urlContext.setProperty(entry.getKey(), value);
        }
      }
      return urlContext;
    }
    catch (Exception ex) {
      // let the query proceed even if we cannot parse the context parameters
      LOG.warn(ex, "Failed to get the context parameters from URL params [%s]", urlParams);
      return null;
    }
  }
}
