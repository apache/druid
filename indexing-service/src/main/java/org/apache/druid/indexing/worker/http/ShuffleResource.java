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

package org.apache.druid.indexing.worker.http;

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.indexing.worker.IntermediaryDataManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.joda.time.Interval;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * HTTP endpoints for shuffle system. The MiddleManager and Indexer use this resource to serve intermediary shuffle
 * data.
 *
 * We use {@link StateResourceFilter} here because it performs an admin-like authorization and
 * all endpoints here are supposed to be used for only internal communcation.
 * Another possible alternate could be performing datasource-level authorization as in TaskResourceFilter.
 * However, datasource information is not available in middleManagers or indexers yet which makes hard to use it.
 * We could develop a new ResourceFileter in the future if needed.
 */
@Path("/druid/worker/v1/shuffle")
@ResourceFilters(StateResourceFilter.class)
public class ShuffleResource
{
  private static final Logger log = new Logger(ShuffleResource.class);

  private final IntermediaryDataManager intermediaryDataManager;

  @Inject
  public ShuffleResource(IntermediaryDataManager intermediaryDataManager)
  {
    this.intermediaryDataManager = intermediaryDataManager;
  }

  @GET
  @Path("/task/{supervisorTaskId}/{subTaskId}/partition")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getPartition(
      @PathParam("supervisorTaskId") String supervisorTaskId,
      @PathParam("subTaskId") String subTaskId,
      @QueryParam("startTime") String startTime,
      @QueryParam("endTime") String endTime,
      @QueryParam("bucketId") int bucketId
  )
  {
    final Interval interval = new Interval(DateTimes.of(startTime), DateTimes.of(endTime));
    final File partitionFile = intermediaryDataManager.findPartitionFile(
        supervisorTaskId,
        subTaskId,
        interval,
        bucketId
    );

    if (partitionFile == null) {
      final String errorMessage = StringUtils.format(
          "Can't find the partition for supervisorTask[%s], subTask[%s], interval[%s], and bucketId[%s]",
          supervisorTaskId,
          subTaskId,
          interval,
          bucketId
      );
      return Response.status(Status.NOT_FOUND).entity(errorMessage).build();
    } else {
      return Response.ok(
          (StreamingOutput) output -> {
            try (final FileInputStream fileInputStream = new FileInputStream(partitionFile)) {
              ByteStreams.copy(fileInputStream, output);
            }
          }
      ).build();
    }
  }

  @DELETE
  @Path("/task/{supervisorTaskId}")
  public Response deletePartitions(@PathParam("supervisorTaskId") String supervisorTaskId)
  {
    try {
      intermediaryDataManager.deletePartitions(supervisorTaskId);
      return Response.ok(supervisorTaskId).build();
    }
    catch (IOException e) {
      log.error(e, "Error while deleting partitions of supervisorTask[%s]", supervisorTaskId);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
    }
  }
}
