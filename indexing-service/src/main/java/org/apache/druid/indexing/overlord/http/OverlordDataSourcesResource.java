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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Datasource APIs exposed by the Overlord to update segments.
 * Some of these APIs are also exposed by the Coordinator, but they have been
 * deprecated and the Overlord APIs must be used for all update operations.
 */
@Path("/druid/indexer/v1/datasources")
public class OverlordDataSourcesResource
{
  private static final Logger log = new Logger(OverlordDataSourcesResource.class);

  private final SegmentsMetadataManager segmentsMetadataManager;
  private final TaskMaster taskMaster;
  private final AuditManager auditManager;

  @Inject
  public OverlordDataSourcesResource(
      TaskMaster taskMaster,
      SegmentsMetadataManager segmentsMetadataManager,
      AuditManager auditManager
  )
  {
    this.taskMaster = taskMaster;
    this.auditManager = auditManager;
    this.segmentsMetadataManager = segmentsMetadataManager;
  }

  private interface SegmentUpdateOperation
  {
    int perform();
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markAllNonOvershadowedSegmentsAsUsed(
      @PathParam("dataSourceName") final String dataSourceName,
      @Context HttpServletRequest req
  )
  {
    SegmentUpdateOperation operation = () -> segmentsMetadataManager
        .markAsUsedAllNonOvershadowedSegmentsInDataSource(dataSourceName);
    return performSegmentUpdate(dataSourceName, operation);
  }

  @DELETE
  @Path("/{dataSourceName}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response markAllSegmentsAsUnused(
      @PathParam("dataSourceName") final String dataSourceName,
      @Context HttpServletRequest req
  )
  {
    SegmentUpdateOperation operation = () -> segmentsMetadataManager
        .markAsUnusedAllSegmentsInDataSource(dataSourceName);
    final Response response = performSegmentUpdate(dataSourceName, operation);

    final int responseCode = response.getStatus();
    if (responseCode >= 200 && responseCode < 300) {
      auditMarkUnusedOperation(response.getEntity(), dataSourceName, req);
    }

    return response;
  }

  @POST
  @Path("/{dataSourceName}/markUsed")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markNonOvershadowedSegmentsAsUsed(
      @PathParam("dataSourceName") final String dataSourceName,
      final SegmentsToUpdateFilter payload
  )
  {
    if (payload == null || !payload.isValid()) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(SegmentsToUpdateFilter.INVALID_PAYLOAD_ERROR_MESSAGE)
          .build();
    } else {
      SegmentUpdateOperation operation = () -> {
        final Interval interval = payload.getInterval();
        final List<String> versions = payload.getVersions();
        if (interval != null) {
          return segmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(dataSourceName, interval, versions);
        } else {
          final Set<String> segmentIds = payload.getSegmentIds();
          if (segmentIds == null || segmentIds.isEmpty()) {
            return 0;
          }

          // Validate segmentIds
          final List<String> invalidSegmentIds = new ArrayList<>();
          for (String segmentId : segmentIds) {
            if (SegmentId.iteratePossibleParsingsWithDataSource(dataSourceName, segmentId).isEmpty()) {
              invalidSegmentIds.add(segmentId);
            }
          }
          if (!invalidSegmentIds.isEmpty()) {
            throw InvalidInput.exception("Could not parse invalid segment IDs[%s]", invalidSegmentIds);
          }

          return segmentsMetadataManager.markAsUsedNonOvershadowedSegments(dataSourceName, segmentIds);
        }
      };

      return performSegmentUpdate(dataSourceName, operation);
    }
  }

  @POST
  @Path("/{dataSourceName}/markUnused")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response markSegmentsAsUnused(
      @PathParam("dataSourceName") final String dataSourceName,
      final SegmentsToUpdateFilter payload,
      @Context final HttpServletRequest req
  )
  {
    if (payload == null || !payload.isValid()) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(SegmentsToUpdateFilter.INVALID_PAYLOAD_ERROR_MESSAGE)
          .build();
    } else {
      SegmentUpdateOperation operation = () -> {
        final Interval interval = payload.getInterval();
        final List<String> versions = payload.getVersions();
        final int numUpdatedSegments;
        if (interval != null) {
          numUpdatedSegments = segmentsMetadataManager.markAsUnusedSegmentsInInterval(dataSourceName, interval, versions);
        } else {
          final Set<SegmentId> segmentIds = payload.getSegmentIds()
                                                   .stream()
                                                   .map(id -> SegmentId.tryParse(dataSourceName, id))
                                                   .filter(Objects::nonNull)
                                                   .collect(Collectors.toSet());

          // Filter out segmentIds that do not belong to this datasource
          numUpdatedSegments = segmentsMetadataManager.markSegmentsAsUnused(
              segmentIds.stream()
                  .filter(segmentId -> segmentId.getDataSource().equals(dataSourceName))
                  .collect(Collectors.toSet())
          );
        }
        auditMarkUnusedOperation(payload, dataSourceName, req);
        return numUpdatedSegments;
      };
      return performSegmentUpdate(dataSourceName, operation);
    }
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markSegmentAsUsed(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    SegmentUpdateOperation operation =
        () -> segmentsMetadataManager.markSegmentAsUsed(segmentId) ? 1 : 0;
    return performSegmentUpdate(dataSourceName, operation);
  }

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response markSegmentAsUnused(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentIdString
  )
  {
    final SegmentId segmentId = SegmentId.tryParse(dataSourceName, segmentIdString);
    if (segmentId == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          StringUtils.format("Could not parse Segment ID[%s] for DataSource[%s]", segmentIdString, dataSourceName)
      ).build();
    }

    SegmentUpdateOperation operation =
        () -> segmentsMetadataManager.markSegmentAsUnused(segmentId) ? 1 : 0;
    return performSegmentUpdate(dataSourceName, operation);
  }

  private Response performSegmentUpdate(String dataSourceName, SegmentUpdateOperation operation)
  {
    if (!taskMaster.isHalfOrFullLeader()) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("I am not leader").build();
    }

    try {
      int numChangedSegments = operation.perform();
      return Response.ok(new SegmentUpdateResponse(numChangedSegments)).build();
    }
    catch (DruidException e) {
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      log.error(e, "Error occurred while updating segments for datasource[%s]", dataSourceName);
      return Response
          .serverError()
          .entity(ImmutableMap.of("error", "Server error", "message", Throwables.getRootCause(e).toString()))
          .build();
    }
  }

  private void auditMarkUnusedOperation(
      Object auditPayload,
      String dataSourceName,
      HttpServletRequest request
  )
  {
    auditManager.doAudit(
        AuditEntry.builder()
                  .key(dataSourceName)
                  .type("segment.markUnused")
                  .payload(auditPayload)
                  .auditInfo(AuthorizationUtils.buildAuditInfo(request))
                  .request(AuthorizationUtils.buildRequestInfo("overlord", request))
                  .build()
    );
  }
}
