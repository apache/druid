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

package org.apache.druid.server.security;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Set of built-in and 'registered' {@link Resource} types for use by {@link Authorizer}
 */
public class ResourceType
{
  public static final String DATASOURCE = "DATASOURCE";
  public static final String VIEW = "VIEW";
  public static final String CONFIG = "CONFIG";
  public static final String STATE = "STATE";
  public static final String SYSTEM_TABLE = "SYSTEM_TABLE";
  public static final String QUERY_CONTEXT = "QUERY_CONTEXT";
  public static final String EXTERNAL = "EXTERNAL";

  private static final Set<String> KNOWN_TYPES = Sets.newConcurrentHashSet();

  // initialize built-ins
  static {
    registerResourceType(DATASOURCE);
    registerResourceType(VIEW);
    registerResourceType(CONFIG);
    registerResourceType(STATE);
    registerResourceType(SYSTEM_TABLE);
    registerResourceType(QUERY_CONTEXT);
    registerResourceType(EXTERNAL);
  }

  /**
   * Set of 'known' {@link Resource} types which have been registered with {@link #registerResourceType}, for use by
   * utility methods looking to construct permission sets for all types (e.g. 'superuser' permission set)
   */
  public static Set<String> knownTypes()
  {
    return KNOWN_TYPES;
  }

  /**
   * 'register' a 'known' type of {@link Resource} to make available via {@link #knownTypes()}
   */
  public static void registerResourceType(String type)
  {
    KNOWN_TYPES.add(type);
  }
}
