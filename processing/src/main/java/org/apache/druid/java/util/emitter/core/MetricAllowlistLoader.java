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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public final class MetricAllowlistLoader
{

  public static final String DEFAULT_METRIC_ALLOWLIST_PATH = "defaultLoggingMetrics.json";

  public static Set<String> loadAllowlist(
      ObjectMapper mapper,
      String allowlistPath,
      MetricAllowlistParser parser
  )
  {
    try (final InputStream is = openAllowlistFile(allowlistPath)) {
      return parser.parse(mapper.readTree(is), allowlistPath);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric allowlist file [%s]", allowlistPath);
    }
  }

  private static InputStream openAllowlistFile(String allowlistPath)
  {
    if (Strings.isNullOrEmpty(allowlistPath)) {
      throw new IAE("Metric allowlist file path is empty");
    }
    try {
      return new FileInputStream(allowlistPath);
    }
    catch (FileNotFoundException e) {
      final InputStream classpathInputStream = MetricAllowlistLoader.class.getClassLoader().getResourceAsStream(allowlistPath);
      if (classpathInputStream != null) {
        return classpathInputStream;
      }
      throw new IAE(e, "Metric allowlist file [%s] not found", allowlistPath);
    }
  }
}
