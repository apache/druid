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

package org.apache.druid.msq.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.query.QueryContexts;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum MSQMode
{
  NON_STRICT_MODE("nonStrict", ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1)),
  STRICT_MODE("strict", ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0));

  private final String value;
  private final Map<String, Object> defaultQueryContext;

  private static final Logger log = new Logger(MSQMode.class);

  MSQMode(final String value, final Map<String, Object> defaultQueryContext)
  {
    this.value = value;
    this.defaultQueryContext = new HashMap<>(defaultQueryContext);
  }

  @Nullable
  public static MSQMode fromString(String str)
  {
    for (MSQMode msqMode : MSQMode.values()) {
      if (msqMode.value.equalsIgnoreCase(str)) {
        return msqMode;
      }
    }
    return null;
  }

  @Override
  public String toString()
  {
    return value;
  }

  public static void populateDefaultQueryContext(final String modeStr, final Map<String, Object> originalQueryContext)
  {
    MSQMode mode = MSQMode.fromString(modeStr);
    if (mode == null) {
      throw new ISE(
          "%s is an unknown multi stage query mode. Acceptable modes: %s",
          modeStr,
          Arrays.stream(MSQMode.values()).map(m -> m.value).collect(Collectors.toList())
      );
    }
    log.debug("Populating default query context with %s for the %s multi stage query mode", mode.defaultQueryContext, mode);
    QueryContexts.addDefaults(originalQueryContext, mode.defaultQueryContext);
  }
}
