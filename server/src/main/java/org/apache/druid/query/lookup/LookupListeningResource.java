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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.server.http.security.LookupResourceFilter;
import org.apache.druid.server.listener.resource.AbstractListenerHandler;
import org.apache.druid.server.listener.resource.ListenerResource;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
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
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Path(ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)
@ResourceFilters(LookupResourceFilter.class)
class LookupListeningResource extends ListenerResource
{
  private static final Logger LOG = new Logger(LookupListeningResource.class);

  private static final TypeReference<LookupsState<Object>> LOOKUPS_STATE_GENERIC_REFERENCE =
      new TypeReference<>() {};

  @Inject
  public LookupListeningResource(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final LookupReferencesManager manager,
      final Provider<HttpServletRequest> requestProvider,
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig,
      final LookupNodeService lookupNodeService
  )
  {
    super(
        jsonMapper,
        smileMapper,
        new AbstractListenerHandler<LookupExtractorFactory>(new TypeReference<>() {})
        {
          @Override
          public Response handleUpdates(InputStream inputStream, ObjectMapper mapper)
          {
            final LookupsState<Object> stateGeneric;
            final LookupsState<LookupExtractorFactoryContainer> state;
            final Map<String, LookupExtractorFactoryContainer> current;
            final Map<String, LookupExtractorFactoryContainer> toLoad;
            try {
              stateGeneric = mapper.readValue(inputStream, LOOKUPS_STATE_GENERIC_REFERENCE);
              current = LookupUtils.tryConvertObjectMapToLookupConfigMap(
                  stateGeneric.getCurrent(),
                  mapper
              );
              toLoad = LookupUtils.tryConvertObjectMapToLookupConfigMap(
                  stateGeneric.getToLoad(),
                  mapper
              );

              state = new LookupsState<>(current, toLoad, stateGeneric.getToDrop());
            }
            catch (final IOException ex) {
              LOG.debug(ex, "Bad request");
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ServletResourceUtils.sanitizeException(ex))
                             .build();
            }

            try {
              final HttpServletRequest request = requestProvider.get();
              final String tier = lookupNodeService.getLookupTier();
              final Set<String> lookupNames = new LinkedHashSet<>(state.getToLoad().keySet());
              lookupNames.addAll(state.getToDrop());
              authorizeChanges(request, tier, lookupNames, authorizerMapper, authConfig);
              state.getToLoad().forEach(manager::add);
              state.getToDrop().forEach(lookName -> {
                manager.remove(lookName, state.getToLoad().getOrDefault(lookName, null));
              });

              return Response.status(Response.Status.ACCEPTED)
                             .entity(readableState(
                                 request,
                                 tier,
                                 manager.getAllLookupsState(),
                                 authorizerMapper,
                                 authConfig,
                                 true
                             ))
                             .build();
            }
            catch (ForbiddenException e) {
              throw e;
            }
            catch (Exception e) {
              LOG.error(e, "Error handling request");
              return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
            }
          }

          @Override
          public Object post(final Map<String, LookupExtractorFactory> lookups)
          {
            final HttpServletRequest request = requestProvider.get();
            // Single lookup routes were already checked by LookupResourceFilter.
            if (request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED) == null) {
              authorizeChanges(
                  request,
                  lookupNodeService.getLookupTier(),
                  lookups.keySet(),
                  authorizerMapper,
                  authConfig
              );
            }
            final Map<String, LookupExtractorFactory> failedUpdates = new HashMap<>();
            for (final Map.Entry<String, LookupExtractorFactory> lookup : lookups.entrySet()) {

              final LookupExtractorFactoryContainer factoryContainer = new LookupExtractorFactoryContainer(
                  null,
                  lookup.getValue()
              );

              manager.add(lookup.getKey(), factoryContainer);
            }
            return ImmutableMap.of("status", "accepted", LookupModule.FAILED_UPDATES_KEY, failedUpdates);
          }

          @Override
          public Object get(String id)
          {
            return manager.get(id).orElse(null);
          }

          @Override
          public LookupsState<LookupExtractorFactoryContainer> getAll()
          {
            return readableState(
                requestProvider.get(),
                lookupNodeService.getLookupTier(),
                manager.getAllLookupsState(),
                authorizerMapper,
                authConfig,
                false
            );
          }

          @Override
          public Object delete(String id)
          {
            manager.remove(id, null);
            return id;
          }
        }
    );
  }

  /**
   * Checks the lookup names in an update before the manager changes node state.
   */
  static void authorizeChanges(
      HttpServletRequest request,
      String tier,
      Set<String> lookups,
      AuthorizerMapper authorizerMapper,
      AuthConfig authConfig
  )
  {
    if (!authConfig.isEnableLookupAuthorization()) {
      return;
    }
    final List<ResourceAction> actions = lookups.stream()
                                                .map(lookup -> lookupAction(
                                                    LookupResourceFilter.resourceName(tier, lookup),
                                                    Action.WRITE
                                                ))
                                                .collect(Collectors.toList());
    if (actions.isEmpty()) {
      actions.add(lookupAction(LookupResourceFilter.resourceName(tier, null), Action.WRITE));
    }
    final AuthorizationResult result = AuthorizationUtils.authorizeAllResourceActions(
        request,
        actions,
        authorizerMapper
    );
    if (!result.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(result.getErrorMessage());
    }
  }

  /**
   * Filters a node state by READ grants. A write response uses the existing request identity
   * because its WRITE check already marked the request as authorized.
   */
  static <T> LookupsState<T> readableState(
      HttpServletRequest request,
      String tier,
      LookupsState<T> state,
      AuthorizerMapper authorizerMapper,
      AuthConfig authConfig,
      boolean writeResponse
  )
  {
    if (!authConfig.isEnableLookupAuthorization()
        || writeResponse && request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH) != null) {
      return state;
    }
    final Set<String> names = new LinkedHashSet<>();
    state.getCurrent().keySet().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
    state.getToLoad().keySet().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
    state.getToDrop().forEach(lookup -> names.add(LookupResourceFilter.resourceName(tier, lookup)));
    if (names.isEmpty()) {
      names.add(LookupResourceFilter.resourceName(tier, null));
    }
    final Set<String> readable = new LinkedHashSet<>();
    final Iterable<String> allowed = writeResponse
                                                ? AuthorizationUtils.filterAuthorizedResources(
                                                    AuthorizationUtils.authenticationResultFromRequest(request),
                                                    names,
                                                    name -> ImmutableList.of(lookupAction(name, Action.READ)),
                                                    authorizerMapper
                                                )
                                                : AuthorizationUtils.filterAuthorizedResources(
                                                    request,
                                                    names,
                                                    name -> ImmutableList.of(lookupAction(name, Action.READ)),
                                                    authorizerMapper
                                                );
    allowed.forEach(readable::add);
    final Map<String, T> current = new LinkedHashMap<>();
    state.getCurrent().forEach((lookup, value) -> {
      if (readable.contains(LookupResourceFilter.resourceName(tier, lookup))) {
        current.put(lookup, value);
      }
    });
    final Map<String, T> toLoad = new LinkedHashMap<>();
    state.getToLoad().forEach((lookup, value) -> {
      if (readable.contains(LookupResourceFilter.resourceName(tier, lookup))) {
        toLoad.put(lookup, value);
      }
    });
    final Set<String> toDrop = new LinkedHashSet<>(state.getToDrop());
    toDrop.removeIf(lookup -> !readable.contains(LookupResourceFilter.resourceName(tier, lookup)));
    return new LookupsState<>(current, toLoad, toDrop);
  }

  private static ResourceAction lookupAction(String name, Action action)
  {
    return new ResourceAction(new Resource(name, ResourceType.LOOKUP), action);
  }
}
