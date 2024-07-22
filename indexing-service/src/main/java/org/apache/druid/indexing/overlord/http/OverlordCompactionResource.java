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

package org.apache.druid.indexing.overlord.http;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;

@Path("/druid/indexer/v1/compaction")
public class OverlordCompactionResource
{
  private static final Logger log = new Logger(OverlordCompactionResource.class);

  private final CompactionScheduler scheduler;

  @Inject
  public OverlordCompactionResource(
      CompactionScheduler scheduler
  )
  {
    this.scheduler = scheduler;
    log.info("Creating the new overlord compaction resource.");
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionProgress(
      @QueryParam("dataSource") String dataSource
  )
  {
    final Long notCompactedSegmentSizeBytes = scheduler.getSegmentBytesYetToBeCompacted(dataSource);
    if (notCompactedSegmentSizeBytes == null) {
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(Collections.singletonMap("error", "Unknown DataSource"))
                     .build();
    } else {
      return Response.ok(Collections.singletonMap("remainingSegmentSize", notCompactedSegmentSizeBytes))
                     .build();
    }
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionSnapshotForDataSource(
      @QueryParam("dataSource") String dataSource
  )
  {
    final Collection<AutoCompactionSnapshot> snapshots;
    if (dataSource == null || dataSource.isEmpty()) {
      snapshots = scheduler.getAllCompactionSnapshots().values();
    } else {
      AutoCompactionSnapshot autoCompactionSnapshot = scheduler.getCompactionSnapshot(dataSource);
      if (autoCompactionSnapshot == null) {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(Collections.singletonMap("error", "Unknown DataSource"))
                       .build();
      }
      snapshots = Collections.singleton(autoCompactionSnapshot);
    }
    return Response.ok(Collections.singletonMap("latestStatus", snapshots)).build();
  }
}
