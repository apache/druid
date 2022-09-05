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

package org.apache.druid.msq.indexing;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.utils.CloseableUtils;

import javax.servlet.http.HttpServletRequest;
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
import java.io.IOException;
import java.io.InputStream;

public class WorkerChatHandler implements ChatHandler
{
  private static final Logger log = new Logger(WorkerChatHandler.class);

  /**
   * Callers must be able to store an entire chunk in memory. It can't be too large.
   */
  private static final long CHANNEL_DATA_CHUNK_SIZE = 1_000_000;

  private final Worker worker;
  private final MSQWorkerTask task;
  private final TaskToolbox toolbox;

  public WorkerChatHandler(TaskToolbox toolbox, Worker worker)
  {
    this.worker = worker;
    this.task = worker.task();
    this.toolbox = toolbox;
  }

  /**
   * Returns up to {@link #CHANNEL_DATA_CHUNK_SIZE} bytes of stage output data.
   *
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
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    try {
      final InputStream inputStream = worker.readChannel(queryId, stageNumber, partitionNumber, offset);
      if (inputStream == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      final Response.ResponseBuilder responseBuilder = Response.ok();

      final byte[] readBuf = new byte[8192];
      final MutableLong bytesReadTotal = new MutableLong(0L);
      final int firstRead = inputStream.read(readBuf);

      if (firstRead == -1) {
        // Empty read means we're at the end of the channel. Set the last fetch header so the client knows this.
        inputStream.close();
        return responseBuilder
            .header(
                FrameFileHttpResponseHandler.HEADER_LAST_FETCH_NAME,
                FrameFileHttpResponseHandler.HEADER_LAST_FETCH_VALUE
            )
            .entity(ByteArrays.EMPTY_ARRAY)
            .build();
      }

      return Response.ok((StreamingOutput) output -> {
        try {
          int bytesReadThisCall = firstRead;
          do {
            final int bytesToWrite =
                (int) Math.min(CHANNEL_DATA_CHUNK_SIZE - bytesReadTotal.longValue(), bytesReadThisCall);
            output.write(readBuf, 0, bytesToWrite);
            bytesReadTotal.add(bytesReadThisCall);
          } while (bytesReadTotal.longValue() < CHANNEL_DATA_CHUNK_SIZE
                   && (bytesReadThisCall = inputStream.read(readBuf)) != -1);
        }
        catch (Throwable e) {
          // Suppress the exception to ensure nothing gets written over the wire once we've sent a 200. The client
          // will resume from where it left off.
          log.noStackTrace().warn(
              e,
              "Error writing channel for query [%s] stage [%s] partition [%s] offset [%,d] to [%s]",
              queryId,
              stageNumber,
              partitionNumber,
              offset,
              req.getRemoteAddr()
          );
        }
        finally {
          CloseableUtils.closeAll(inputStream, output);
        }
      }).build();
    }
    catch (IOException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postWorkOrder} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workOrder")
  public Response httpPostWorkOrder(final WorkOrder workOrder, @Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    worker.postWorkOrder(workOrder);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#postResultPartitionBoundaries} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/resultPartitionBoundaries/{queryId}/{stageNumber}")
  public Response httpPostResultPartitionBoundaries(
      final ClusterByPartitions stagePartitionBoundaries,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    if (worker.postResultPartitionBoundaries(stagePartitionBoundaries, queryId, stageNumber)) {
      return Response.status(Response.Status.ACCEPTED).build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
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
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
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
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    worker.postFinish();
    return Response.status(Response.Status.ACCEPTED).build();
  }


  /**
   * See {@link org.apache.druid.msq.exec.WorkerClient#getCounters} for the client-side code that calls this API.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/counters")
  public Response httpGetCounters(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    return Response.status(Response.Status.OK).entity(worker.getCounters()).build();
  }
}
