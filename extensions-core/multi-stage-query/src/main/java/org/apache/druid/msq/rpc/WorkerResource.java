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

package org.apache.druid.msq.rpc;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.serde.ClusterByStatisticsSnapshotSerde;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerResource
{
  private static final Logger log = new Logger(WorkerResource.class);

  /**
   * Callers must be able to store an entire chunk in memory. It can't be too large.
   */
  private static final long CHANNEL_DATA_CHUNK_SIZE = 1_000_000;
  private static final long GET_CHANNEL_DATA_TIMEOUT = 30_000L;

  protected final Worker worker;
  protected final ResourcePermissionMapper permissionMapper;
  protected final AuthorizerMapper authorizerMapper;

  public WorkerResource(
      final Worker worker,
      final ResourcePermissionMapper permissionMapper,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.worker = worker;
    this.permissionMapper = permissionMapper;
    this.authorizerMapper = authorizerMapper;
  }

  /**
   * Returns up to {@link #CHANNEL_DATA_CHUNK_SIZE} bytes of stage output data.
   * <p>
   * See {@link org.apache.druid.msq.exec.WorkerClient#fetchChannelData} for the client-side code that calls this API.
   */
  @GET
  @Path("/channels/{queryId}/{stageNumber}/{partitionNumber}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response httpGetChannelData(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("partitionNumber") final int partitionNumber,
      @QueryParam("offset") final long offset,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);

    final ListenableFuture<InputStream> dataFuture =
        worker.readStageOutput(new StageId(queryId, stageNumber), partitionNumber, offset);

    final AsyncContext asyncContext = req.startAsync();
    final AtomicBoolean responseResolved = new AtomicBoolean();

    asyncContext.setTimeout(GET_CHANNEL_DATA_TIMEOUT);
    asyncContext.addListener(
        new AsyncListener()
        {
          @Override
          public void onComplete(AsyncEvent event)
          {
          }

          @Override
          public void onTimeout(AsyncEvent event)
          {
            if (responseResolved.compareAndSet(false, true)) {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              event.getAsyncContext().complete();
            }
          }

          @Override
          public void onError(AsyncEvent event)
          {
          }

          @Override
          public void onStartAsync(AsyncEvent event)
          {
          }
        }
    );

    // Save these items, since "req" becomes inaccessible in future exception handlers.
    final String remoteAddr = req.getRemoteAddr();
    final String requestURI = req.getRequestURI();

    Futures.addCallback(
        dataFuture,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(final InputStream inputStream)
          {
            if (!responseResolved.compareAndSet(false, true)) {
              return;
            }

            final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

            try (final OutputStream outputStream = response.getOutputStream()) {
              if (inputStream == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
              } else {
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType(MediaType.APPLICATION_OCTET_STREAM);

                final byte[] readBuf = new byte[8192];
                final int firstRead = inputStream.read(readBuf);

                if (firstRead == -1) {
                  // Empty read means we're at the end of the channel.
                  // Set the last fetch header so the client knows this.
                  response.setHeader(
                      FrameFileHttpResponseHandler.HEADER_LAST_FETCH_NAME,
                      FrameFileHttpResponseHandler.HEADER_LAST_FETCH_VALUE
                  );
                } else {
                  long bytesReadTotal = 0;
                  int bytesReadThisCall = firstRead;
                  do {
                    final int bytesToWrite =
                        (int) Math.min(CHANNEL_DATA_CHUNK_SIZE - bytesReadTotal, bytesReadThisCall);
                    outputStream.write(readBuf, 0, bytesToWrite);
                    bytesReadTotal += bytesReadThisCall;
                  } while (bytesReadTotal < CHANNEL_DATA_CHUNK_SIZE
                           && (bytesReadThisCall = inputStream.read(readBuf)) != -1);
                }
              }
            }
            catch (Exception e) {
              log.noStackTrace().warn(e, "Could not respond to request from[%s] to[%s]", remoteAddr, requestURI);
            }
            finally {
              CloseableUtils.closeAndSuppressExceptions(inputStream, e -> log.warn("Failed to close output channel"));
              asyncContext.complete();
            }
          }

          @Override
          public void onFailure(Throwable e)
          {
            if (responseResolved.compareAndSet(false, true)) {
              try {
                HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                asyncContext.complete();
              }
              catch (Exception e2) {
                e.addSuppressed(e2);
              }

              log.noStackTrace().warn(e, "Request failed from[%s] to[%s]", remoteAddr, requestURI);
            }
          }
        },
        Execs.directExecutor()
    );

    return null;
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postWorkOrder} for the client-side code that calls this API.
   */
  @POST
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/workOrder")
  public Response httpPostWorkOrder(final WorkOrder workOrder, @Context final HttpServletRequest req)
  {
    final String queryId = workOrder.getQueryDefinition().getQueryId();
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);
    worker.postWorkOrder(workOrder);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postResultPartitionBoundaries} for the client-side code that calls this API.
   */
  @POST
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/resultPartitionBoundaries/{queryId}/{stageNumber}")
  public Response httpPostResultPartitionBoundaries(
      final ClusterByPartitions stagePartitionBoundaries,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);
    if (worker.postResultPartitionBoundaries(new StageId(queryId, stageNumber), stagePartitionBoundaries)) {
      return Response.status(Response.Status.ACCEPTED).build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  @POST
  @Path("/keyStatistics/{queryId}/{stageNumber}")
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
  public Response httpFetchKeyStatistics(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @QueryParam("sketchEncoding") @Nullable final SketchEncoding sketchEncoding,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);
    ClusterByStatisticsSnapshot clusterByStatisticsSnapshot;
    StageId stageId = new StageId(queryId, stageNumber);
    try {
      clusterByStatisticsSnapshot = worker.fetchStatisticsSnapshot(stageId);
      if (SketchEncoding.OCTET_STREAM.equals(sketchEncoding)) {
        return Response.status(Response.Status.ACCEPTED)
                       .type(MediaType.APPLICATION_OCTET_STREAM)
                       .entity(
                           (StreamingOutput) output ->
                               ClusterByStatisticsSnapshotSerde.serialize(output, clusterByStatisticsSnapshot)
                       )
                       .build();
      } else {
        return Response.status(Response.Status.ACCEPTED)
                       .type(MediaType.APPLICATION_JSON)
                       .entity(clusterByStatisticsSnapshot)
                       .build();
      }
    }
    catch (Exception e) {
      String errorMessage = StringUtils.format(
          "Invalid request for key statistics for query[%s] and stage[%d]",
          queryId,
          stageNumber
      );
      log.error(e, errorMessage);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", errorMessage))
                     .build();
    }
  }

  @POST
  @Path("/keyStatisticsForTimeChunk/{queryId}/{stageNumber}/{timeChunk}")
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
  public Response httpFetchKeyStatisticsWithSnapshot(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("timeChunk") final long timeChunk,
      @QueryParam("sketchEncoding") @Nullable final SketchEncoding sketchEncoding,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);
    ClusterByStatisticsSnapshot snapshotForTimeChunk;
    StageId stageId = new StageId(queryId, stageNumber);
    try {
      snapshotForTimeChunk = worker.fetchStatisticsSnapshotForTimeChunk(stageId, timeChunk);
      if (SketchEncoding.OCTET_STREAM.equals(sketchEncoding)) {
        return Response.status(Response.Status.ACCEPTED)
                       .type(MediaType.APPLICATION_OCTET_STREAM)
                       .entity(
                           (StreamingOutput) output ->
                               ClusterByStatisticsSnapshotSerde.serialize(output, snapshotForTimeChunk)
                       )
                       .build();
      } else {
        return Response.status(Response.Status.ACCEPTED)
                       .type(MediaType.APPLICATION_JSON)
                       .entity(snapshotForTimeChunk)
                       .build();
      }
    }
    catch (Exception e) {
      String errorMessage = StringUtils.format(
          "Invalid request for key statistics for query[%s], stage[%d] and timeChunk[%d]",
          queryId,
          stageNumber,
          timeChunk
      );
      log.error(e, errorMessage);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", errorMessage))
                     .build();
    }
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postCleanupStage} for the client-side code that calls this API.
   */
  @POST
  @Path("/cleanupStage/{queryId}/{stageNumber}")
  public Response httpPostCleanupStage(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeQueryRequest(permissionMapper, authorizerMapper, req, queryId);
    worker.postCleanupStage(new StageId(queryId, stageNumber));
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postFinish} for the client-side code that calls this API.
   */
  @POST
  @Path("/finish")
  public Response httpPostFinish(@Context final HttpServletRequest req)
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    worker.postFinish();
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#getCounters} for the client-side code that calls this API.
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON + "; qs=0.9", SmileMediaTypes.APPLICATION_JACKSON_SMILE + "; qs=0.1"})
  @Path("/counters")
  public Response httpGetCounters(@Context final HttpServletRequest req)
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    return Response.status(Response.Status.OK).entity(worker.getCounters()).build();
  }

}
