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

package org.apache.druid.testing.metrics;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.testing.cli.CliEventCollector;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Used by {@link CliEventCollector}.
 * <p>
 * Collects metrics emitted by other Druid services using {@code HttpPostEmitter},
 * and forwards them to {@link ServiceEmitter}. This can be used in conjunction
 * with {@code LatchableEmitter} to watch for certain events to be emitted by a
 * specific Druid service.
 */
@Path("/druid-ext/testing-tools/events")
public class EventCollectorResource
{
  private static final Logger log = new Logger(EventCollectorResource.class);

  private final ServiceEmitter emitter;

  @Inject
  public EventCollectorResource(
      ServiceEmitter emitter
  )
  {
    this.emitter = emitter;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response postMetrics(
      List<EmittedEvent> events
  )
  {
    final Set<String> services = new HashSet<>();
    final Set<String> hosts = new HashSet<>();

    int collectedEvents = 0;
    for (EmittedEvent event : events) {
      try {
        emitter.emit(event.toEvent());
        ++collectedEvents;

        final String service = event.getStringValue(Event.SERVICE);
        if (service != null) {
          services.add(service);
        }

        final String host = event.getStringValue(Event.HOST);
        if (host != null) {
          hosts.add(host);
        }
      }
      catch (Exception e) {
        log.noStackTrace().error(e, "Could not collect event[%s]", event);
      }
    }

    log.debug("Collected [%d] events from service[%s], host[%s].", collectedEvents, services, hosts);
    return Response.ok("{}").build();
  }
}
