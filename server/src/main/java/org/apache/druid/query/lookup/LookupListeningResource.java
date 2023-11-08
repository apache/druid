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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.common.utils.ServletResourceUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.listener.resource.AbstractListenerHandler;
import org.apache.druid.server.listener.resource.ListenerResource;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;

import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Path(ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)
@ResourceFilters(ConfigResourceFilter.class)
class LookupListeningResource extends ListenerResource
{
  private static final Logger LOG = new Logger(LookupListeningResource.class);

  private static final TypeReference<LookupsState<Object>> LOOKUPS_STATE_GENERIC_REFERENCE =
      new TypeReference<LookupsState<Object>>()
      {
      };

  @Inject
  public LookupListeningResource(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final LookupReferencesManager manager
  )
  {
    super(
        jsonMapper,
        smileMapper,
        new AbstractListenerHandler<LookupExtractorFactory>(new TypeReference<LookupExtractorFactory>()
        {
        })
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
              state.getToLoad().forEach(manager::add);
              state.getToDrop().forEach(lookName -> {
                manager.remove(lookName, state.getToLoad().getOrDefault(lookName, null));
              });

              return Response.status(Response.Status.ACCEPTED).entity(manager.getAllLookupsState()).build();
            }
            catch (Exception e) {
              LOG.error(e, "Error handling request");
              return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
            }
          }

          @Override
          public Object post(final Map<String, LookupExtractorFactory> lookups)
          {
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
            return manager.getAllLookupsState();
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
}
