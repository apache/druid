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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.http.security.LookupResourceFilter;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shared authorization and response filtering for lookup HTTP endpoints.
 * Callers retain the legacy CONFIG authorization path when lookup authorization is disabled.
 */
public final class LookupAuthorizationUtils
{
  private LookupAuthorizationUtils()
  {
  }

  /**
   * Authorizes every action before a lookup endpoint accesses or changes the requested resources.
   */
  public static void authorizeResourceActions(
      final HttpServletRequest request,
      final Iterable<ResourceAction> actions,
      final AuthorizerMapper authorizerMapper
  )
  {
    final AuthorizationResult result = AuthorizationUtils.authorizeAllResourceActions(request, actions, authorizerMapper);
    if (!result.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(result.getErrorMessage());
    }
  }

  /**
   * Filters resource names by READ grants. A write response uses
   * the existing request identity because the WRITE check already marked the request as authorized.
   */
  public static Set<String> readableNames(
      final HttpServletRequest request,
      final Set<String> names,
      final AuthorizerMapper authorizerMapper,
      final boolean writeResponse
  )
  {
    if (names.isEmpty()) {
      names.add(LookupResourceFilter.resourceName(null, null));
    }
    return ImmutableSet.copyOf(writeResponse
                               ? AuthorizationUtils.filterAuthorizedResources(
                                   AuthorizationUtils.authenticationResultFromRequest(request),
                                   names,
                                   name -> ImmutableList.of(
                                       new ResourceAction(new Resource(name, ResourceType.LOOKUP), Action.READ)
                                   ),
                                   authorizerMapper
                               )
                               : AuthorizationUtils.filterAuthorizedResources(
                                   request,
                                   names,
                                   name -> ImmutableList.of(
                                       new ResourceAction(new Resource(name, ResourceType.LOOKUP), Action.READ)
                                   ),
                                   authorizerMapper
                               ));
  }

  /**
   * Collects lookup names without adding a tier fallback, so multiple node states can be combined.
   */
  public static void addStateNames(final Set<String> names, final String tier, final LookupsState<?> state)
  {
    state.getCurrent().keySet().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
    state.getToLoad().keySet().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
    state.getToDrop().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
  }

  /**
   * Copies only lookup entries whose policy names are readable.
   */
  public static <T> Map<String, T> filterLookups(final Set<String> readable, final String tier, final Map<String, T> lookups)
  {
    final Map<String, T> result = new LinkedHashMap<>();
    lookups.forEach((lookup, value) -> {
      if (readable.contains(LookupResourceFilter.resourceName(tier, lookup))) {
        result.put(lookup, value);
      }
    });
    return result;
  }

  /**
   * Removes unreadable lookups from current, toLoad, and toDrop without changing the original state.
   */
  public static <T> LookupsState<T> filterState(final Set<String> readable, final String tier, final LookupsState<T> state)
  {
    final Set<String> toDrop = new LinkedHashSet<>(state.getToDrop());
    toDrop.removeIf(lookup -> !readable.contains(LookupResourceFilter.resourceName(tier, lookup)));
    return new LookupsState<>(
        filterLookups(readable, tier, state.getCurrent()),
        filterLookups(readable, tier, state.getToLoad()),
        toDrop
    );
  }
}
