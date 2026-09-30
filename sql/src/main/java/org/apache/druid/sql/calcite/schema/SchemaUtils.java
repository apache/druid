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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.error.DruidException;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SchemaUtils
{
  private SchemaUtils()
  {
    // No instantiation.
  }

  /**
   * Returns whether a given table with resource type {@code resourceType} should be visible.
   */
  public static boolean isTableVisible(
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final String tableName,
      final String resourceType
  )
  {
    return !filterVisibleTables(authorizerMapper, authenticationResult, Set.of(tableName), resourceType).isEmpty();
  }

  /**
   * Returns the set of visible table names, given tables with resource type {@code resourceType}.
   */
  public static Set<String> filterVisibleTables(
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final Iterable<String> tableNames,
      final String resourceType
  )
  {
    if (resourceType == null) {
      throw DruidException.defensive("Null resource type not expected");
    }

    return filterVisibleResources(
        authorizerMapper,
        authenticationResult,
        tableNames,
        name -> new Resource(name, resourceType)
    );
  }

  /**
   * Like {@link #filterVisibleTables}, but each name is mapped to the {@link Resource} to authorize against, which
   * need not be named after the object itself. A null resource means the name does not need authorization.
   */
  public static Set<String> filterVisibleResources(
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final Iterable<String> names,
      final Function<String, Resource> resourceFn
  )
  {
    final Set<String> visibleNames = new LinkedHashSet<>();
    final Map<Resource, Set<String>> namesByResource = new LinkedHashMap<>();

    for (final String name : names) {
      final Resource resource = resourceFn.apply(name);
      if (resource == null) {
        visibleNames.add(name);
      } else {
        namesByResource.computeIfAbsent(resource, _ -> new LinkedHashSet<>()).add(name);
      }
    }

    final Iterable<Resource> authorizedResources = AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        namesByResource.keySet(),
        resource -> List.of(new ResourceAction(resource, Action.READ)),
        authorizerMapper
    );

    for (final Resource resource : authorizedResources) {
      visibleNames.addAll(namesByResource.get(resource));
    }

    return visibleNames;
  }
}
