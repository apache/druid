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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A config class that applies to all JDBC connections to other databases.
 *
 * @see org.apache.druid.utils.ConnectionUriUtils
 */
public class JdbcAccessSecurityConfig
{
  static final Set<String> DEFAULT_ALLOWED_PROPERTIES = ImmutableSet.of(
      // MySQL
      "useSSL",
      "requireSSL",

      // PostgreSQL
      "ssl",
      "sslmode"
  );

  /**
   * Prefixes of the properties that can be added automatically by {@link java.sql.Driver} during
   * connection URL parsing. Any properties resulted by connection URL parsing are regarded as
   * system properties if they start with the prefixes in this set.
   * Only these non-system properties are checkaed against {@link #getAllowedProperties()}.
   */
  private static final Set<String> SYSTEM_PROPERTY_PREFIXES = ImmutableSet.of(
      // MySQL
      // There can be multiple host and port properties if multiple addresses are specified.
      // The pattern of the property name is HOST.i and PORT.i where i is an integer.
      "HOST",
      "PORT",
      "NUM_HOSTS",
      "DBNAME",

      // PostgreSQL
      "PGHOST",
      "PGPORT",
      "PGDBNAME"
  );

  @JsonProperty
  private Set<String> allowedProperties = DEFAULT_ALLOWED_PROPERTIES;

  @JsonProperty
  private boolean allowUnknownJdbcUrlFormat = true;

  // Enforcing allow list check can break rolling upgrade. This is not good for patch releases
  // and is why this config is added. However, from the security point of view, this config
  // should be always enabled in production to secure your cluster. As a result, this config
  // is deprecated and will be removed in the near future.
  @Deprecated
  @JsonProperty
  private boolean enforceAllowedProperties = false;

  @JsonIgnore
  public Set<String> getSystemPropertyPrefixes()
  {
    return SYSTEM_PROPERTY_PREFIXES;
  }

  public Set<String> getAllowedProperties()
  {
    return allowedProperties;
  }

  public boolean isAllowUnknownJdbcUrlFormat()
  {
    return allowUnknownJdbcUrlFormat;
  }

  public boolean isEnforceAllowedProperties()
  {
    return enforceAllowedProperties;
  }
}
