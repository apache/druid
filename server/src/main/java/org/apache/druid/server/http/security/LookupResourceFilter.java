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

package org.apache.druid.server.http.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Use this resource filter for API endpoints that contain {@code lookups} in their request path.
 *
 * The resource name is {@code tier/lookup}, or {@code tier} when the tier has no lookups.
 * Percent signs and slashes in each component are escaped before joining them.
 * Coordinator-wide requests with no tier or lookup use {@code LOOKUP:LOOKUP}.
 * When lookup authorization is disabled, all requests use {@code CONFIG:CONFIG}.
 *
 * Here are some example paths where this filter is used -
 * - druid/coordinator/v1/lookups/config/{tier}/{lookup}
 * - druid/coordinator/v1/lookups/status/{tier}/{lookup}
 * - druid/coordinator/v1/lookups/config/{tier}
 * - druid/coordinator/v1/lookups/config/all
 */
public class LookupResourceFilter extends AbstractResourceFilter
{
  private static final String BULK_FILTER = LookupResourceFilter.class.getName() + ".bulkFilter";

  private final boolean enabled;
  private final Injector injector;

  @Context
  UriInfo uriInfo;

  @Inject
  public LookupResourceFilter(AuthorizerMapper authorizerMapper, AuthConfig authConfig, Injector injector)
  {
    super(authorizerMapper);
    this.enabled = authConfig.isEnableLookupAuthorization();
    this.injector = injector;
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (!enabled) {
      authorize(ImmutableList.of(new ResourceAction(new Resource("CONFIG", ResourceType.CONFIG), getAction(request))));
      return request;
    }
    final MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
    final String lookup = lookupPathParam(pathParams);
    if (lookup == null) {
      // Bulk routes provide their lookup names after reading the request body or response data.
      getReq().setAttribute(BULK_FILTER, this);
    } else {
      authorize(pathParams.getFirst("tier"), lookup, getAction(request));
    }
    return request;
  }

  private static String lookupPathParam(MultivaluedMap<String, String> pathParams)
  {
    for (final String param : ImmutableList.of("lookup", "lookupId", "id")) {
      if (pathParams.containsKey(param)) {
        return pathParams.getFirst(param);
      }
    }
    return null;
  }

  public static <T> T filterBulk(HttpServletRequest request, T value)
  {
    return filterBulk(request, null, value);
  }

  /**
   * Filters a write response using READ permissions instead of the request's WRITE action.
   */
  public static <T> T filterBulkRead(HttpServletRequest request, T value)
  {
    return filterBulk(request, null, value, true);
  }

  /**
   * Checks every lookup in a bulk write or returns only entries allowed by {@link Action#READ} for a GET.
   * An empty value uses the tier name, or {@link ResourceType#LOOKUP} when no tier is available.
   */
  public static <T> T filterBulk(HttpServletRequest request, String tier, T value)
  {
    return filterBulk(request, tier, value, false);
  }

  private static <T> T filterBulk(HttpServletRequest request, String tier, T value, boolean readResponse)
  {
    final LookupResourceFilter filter = request == null ? null : (LookupResourceFilter) request.getAttribute(BULK_FILTER);
    if (filter == null) {
      return value;
    }
    final String method = request.getMethod();
    final Action action = "GET".equals(method) || "HEAD".equals(method) ? Action.READ : Action.WRITE;
    final String targetTier = tier == null && value instanceof LookupsState
                              ? filter.injector.getInstance(LookupNodeService.class).getLookupTier()
                              : tier;
    final Set<String> names = new LinkedHashSet<>();
    addNames(names, targetTier, value);
    if (names.isEmpty()) {
      names.add(name(targetTier, null));
    }
    if (readResponse) {
      // The write already marked this request as authorized. Check READ without marking it again.
      if (request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
        return value;
      }
      final Set<String> readable = ImmutableSet.copyOf(AuthorizationUtils.filterAuthorizedResources(
          AuthorizationUtils.authenticationResultFromRequest(request),
          names,
          target -> ImmutableList.of(lookupAction(target, Action.READ)),
          filter.getAuthorizerMapper()
      ));
      return readable(readable, targetTier, value);
    }
    if ("GET".equals(method)) {
      return readable(filter.readableNames(request, names), targetTier, value);
    }
    filter.authorize(Iterables.transform(names, target -> lookupAction(target, action)));
    return value;
  }

  /**
   * Returns whether this request uses lookup authorization for a bulk route.
   */
  public static boolean isBulkAuthorizationEnabled(HttpServletRequest request)
  {
    return request != null && request.getAttribute(BULK_FILTER) != null;
  }

  /**
   * Filters a tier listing using known lookup names and tier grants where no lookups are known.
   */
  public static Set<String> filterBulk(
      HttpServletRequest request,
      Set<String> tiers,
      Map<String, ? extends Map<String, ?>> knownLookups
  )
  {
    final LookupResourceFilter filter = request == null ? null : (LookupResourceFilter) request.getAttribute(BULK_FILTER);
    if (filter == null) {
      return tiers;
    }
    final Set<String> names = new LinkedHashSet<>();
    addNames(names, null, knownLookups);
    tiers.forEach(tier -> {
      if (knownLookups == null || !knownLookups.containsKey(tier)) {
        names.add(tier);
      }
    });
    final Set<String> readable = filter.readableNames(request, names);
    final Set<String> filtered = new LinkedHashSet<>();
    tiers.forEach(tier -> {
      if (canReadTier(readable, tier)) {
        filtered.add(tier);
      }
    });
    return filtered;
  }

  private Set<String> readableNames(HttpServletRequest request, Set<String> names)
  {
    if (names.isEmpty()) {
      names.add(name(null, null));
    }
    return ImmutableSet.copyOf(AuthorizationUtils.filterAuthorizedResources(
        request,
        names,
        target -> ImmutableList.of(lookupAction(target, Action.READ)),
        getAuthorizerMapper()
    ));
  }

  private void authorize(Iterable<ResourceAction> actions)
  {
    final AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        getReq(), actions, getAuthorizerMapper()
    );
    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }
  }

  /**
   * Uses the serving node's tier for lookup routes without a tier path parameter.
   */
  private void authorize(String tier, String lookup, Action action)
  {
    final String targetTier = tier == null ? injector.getInstance(LookupNodeService.class).getLookupTier() : tier;
    authorize(ImmutableList.of(lookupAction(name(targetTier, lookup), action)));
  }

  private static ResourceAction lookupAction(String name, Action action)
  {
    return new ResourceAction(new Resource(name, ResourceType.LOOKUP), action);
  }

  private static String name(String tier, String lookup)
  {
    return tier == null ? ResourceType.LOOKUP : lookup == null ? escape(tier) : escape(tier) + "/" + escape(lookup);
  }

  // Escape each component so a slash inside a name cannot match a tier boundary.
  private static String escape(String value)
  {
    return value.replace("%", "%25").replace("/", "%2F");
  }

  /**
   * Collects resource names from tier maps, nested node maps, and {@link LookupsState} values.
   */
  private static void addNames(Set<String> names, String tier, Object value)
  {
    if (value instanceof LookupsState) {
      final LookupsState<?> state = (LookupsState<?>) value;
      addNames(names, tier, state.getCurrent());
      addNames(names, tier, state.getToLoad());
      addNames(names, tier, state.getToDrop());
    } else if (value instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) value;
      // Root map keys are tiers. Within a tier, String keys are lookups and other keys are nodes.
      if (tier == null) {
        map.forEach((key, entry) -> {
          final int size = names.size();
          addNames(names, (String) key, entry);
          if (names.size() == size) {
            names.add(name((String) key, null));
          }
        });
      } else {
        map.forEach((key, entry) -> {
          if (key instanceof String) {
            names.add(name(tier, (String) key));
          } else {
            addNames(names, tier, entry);
          }
        });
      }
    } else if (value instanceof Collection) {
      ((Collection<?>) value).forEach(lookup -> names.add(name(tier, (String) lookup)));
    }
  }

  private static boolean canReadTier(Set<String> readable, String tier)
  {
    final String name = name(tier, null);
    return readable.contains(name) || readable.stream().anyMatch(target -> target.startsWith(name + "/"));
  }

  /**
   * Retains authorized entries while preserving the map or {@link LookupsState} response structure.
   */
  @SuppressWarnings("unchecked")
  static <T> T readable(Set<String> allowed, String tier, T value)
  {
    if (value instanceof LookupsState) {
      final LookupsState<?> state = (LookupsState<?>) value;
      final Map<String, Object> current = (Map<String, Object>) readable(allowed, tier, state.getCurrent());
      final Map<String, Object> toLoad = (Map<String, Object>) readable(allowed, tier, state.getToLoad());
      final Set<String> toDrop = new LinkedHashSet<>(state.getToDrop());
      toDrop.removeIf(lookup -> !allowed.contains(name(tier, lookup)));
      return (T) new LookupsState<>(current, toLoad, toDrop);
    }
    if (value instanceof Map) {
      final Map<Object, Object> result = new LinkedHashMap<>();
      if (tier != null && !canReadTier(allowed, tier)) {
        return (T) result;
      }
      ((Map<?, ?>) value).forEach((key, entry) -> {
        // String keys within a tier are lookup names.
        if (tier != null && key instanceof String) {
          if (allowed.contains(name(tier, (String) key))) {
            result.put(key, entry);
          }
          return;
        }

        // The outer map supplies the tier when the route does not.
        final String entryTier = tier == null ? (String) key : tier;
        if (tier == null && !canReadTier(allowed, entryTier)) {
          return;
        }
        final Object filtered = readable(allowed, entryTier, entry);
        // Do not reveal a node with no readable lookups unless the tier itself is readable.
        if (filtered instanceof LookupsState && !allowed.contains(name(entryTier, null))) {
          final LookupsState<?> state = (LookupsState<?>) filtered;
          if (state.getCurrent().isEmpty() && state.getToLoad().isEmpty() && state.getToDrop().isEmpty()) {
            return;
          }
        }
        result.put(key, filtered);
      });
      return (T) result;
    }
    return value;
  }
}
