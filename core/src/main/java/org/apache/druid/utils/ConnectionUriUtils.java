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

package org.apache.druid.utils;

import com.google.common.base.Preconditions;

import java.util.Set;

public final class ConnectionUriUtils
{
  // Note: MySQL JDBC connector 8 supports 7 other protocols than just `jdbc:mysql:`
  // (https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html).
  // We should consider either expanding recognized mysql protocols or restricting allowed protocols to
  // just a basic one.
  public static final String MYSQL_PREFIX = "jdbc:mysql:";
  public static final String POSTGRES_PREFIX = "jdbc:postgresql:";

  /**
   * This method checks {@param actualProperties} against {@param allowedProperties} if they are not system properties.
   * A property is regarded as a system property if its name starts with a prefix in {@param systemPropertyPrefixes}.
   * See org.apache.druid.server.initialization.JDBCAccessSecurityConfig for more details.
   *
   * If a non-system property that is not allowed is found, this method throws an {@link IllegalArgumentException}.
   */
  public static void throwIfPropertiesAreNotAllowed(
      Set<String> actualProperties,
      Set<String> systemPropertyPrefixes,
      Set<String> allowedProperties
  )
  {
    for (String property : actualProperties) {
      if (systemPropertyPrefixes.stream().noneMatch(property::startsWith)) {
        Preconditions.checkArgument(
            allowedProperties.contains(property),
            "The property [%s] is not in the allowed list %s",
            property,
            allowedProperties
        );
      }
    }
  }

  private ConnectionUriUtils()
  {
  }
}
