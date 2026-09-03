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

import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SchemaUtils
{
  private SchemaUtils()
  {
    // No instantiation.
  }

  public static boolean isTableVisible(
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final String tableName,
      final Function<String, String> resourceTypeFn
  )
  {
    return !filterVisibleTables(authorizerMapper, authenticationResult, Set.of(tableName), resourceTypeFn).isEmpty();
  }

  public static Set<String> filterVisibleTables(
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final Iterable<String> tableNames,
      final Function<String, String> resourceTypeFn
  )
  {
    final Set<String> visibleNames = new LinkedHashSet<>();
    final Set<Resource> authorizableResources = new LinkedHashSet<>();

    for (final String tableName : tableNames) {
      final String resourceType = resourceTypeFn.apply(tableName);
      if (resourceType == null) {
        // No ResourceType means this name does not need authorization. It's always visible.
        visibleNames.add(tableName);
      } else {
        authorizableResources.add(new Resource(tableName, resourceType));
      }
    }

    final Iterable<Resource> authorizedResources = AuthorizationUtils.filterAuthorizedResources(
        authenticationResult,
        authorizableResources,
        resource -> List.of(new ResourceAction(resource, Action.READ)),
        authorizerMapper
    );

    for (final Resource resource : authorizedResources) {
      visibleNames.add(resource.getName());
    }

    return visibleNames;
  }
}
