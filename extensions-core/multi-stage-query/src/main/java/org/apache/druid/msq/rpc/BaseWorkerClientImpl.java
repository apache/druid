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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.frame.file.FrameFilePartialFetch;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;

/**
 * Base worker client. Subclasses override {@link #getClient(String)} and {@link #close()} to build a complete client
 * for talking to specific types of workers.
 */
public abstract class BaseWorkerClientImpl implements WorkerClient
{
  private final ObjectMapper objectMapper;
  private final String contentType;

  protected BaseWorkerClientImpl(final ObjectMapper objectMapper, final String contentType)
  {
    this.objectMapper = objectMapper;
    this.contentType = contentType;
  }

  @Nonnull
  public static String getStagePartitionPath(StageId stageId, int partitionNumber)
  {
    return StringUtils.format(
        "/channels/%s/%d/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        partitionNumber
    );
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerId, WorkOrder workOrder)
  {
    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/workOrder")
            .objectContent(objectMapper, contentType, workOrder),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<ClusterByStatisticsSnapshot> fetchClusterByStatisticsSnapshot(
      String workerId,
      StageId stageId
  )
  {
    String path = StringUtils.format(
        "/keyStatistics/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return FutureUtils.transform(
        getClient(workerId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, path).header(HttpHeaders.ACCEPT, contentType),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<ClusterByStatisticsSnapshot>() {})
    );
  }

  @Override
  public ListenableFuture<ClusterByStatisticsSnapshot> fetchClusterByStatisticsSnapshotForTimeChunk(
      String workerId,
      StageId stageId,
      long timeChunk
  )
  {
    String path = StringUtils.format(
        "/keyStatisticsForTimeChunk/%s/%d/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        timeChunk
    );

    return FutureUtils.transform(
        getClient(workerId).asyncRequest(
            new RequestBuilder(HttpMethod.POST, path).header(HttpHeaders.ACCEPT, contentType),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<ClusterByStatisticsSnapshot>() {})
    );
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    final String path = StringUtils.format(
        "/resultPartitionBoundaries/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .objectContent(objectMapper, contentType, partitionBoundaries),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  /**
   * Client-side method for {@link org.apache.druid.msq.indexing.client.WorkerChatHandler#httpPostCleanupStage}.
   */
  @Override
  public ListenableFuture<Void> postCleanupStage(
      final String workerId,
      final StageId stageId
  )
  {
    final String path = StringUtils.format(
        "/cleanupStage/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, path),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<Void> postFinish(String workerId)
  {
    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/finish"),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String workerId)
  {
    return FutureUtils.transform(
        getClient(workerId).asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/counters").header(HttpHeaders.ACCEPT, contentType),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<CounterSnapshotsTree>() {})
    );
  }

  private static final Logger log = new Logger(BaseWorkerClientImpl.class);

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      String workerId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  )
  {
    final ServiceClient client = getClient(workerId);
    final String path = getStagePartitionPath(stageId, partitionNumber);

    final SettableFuture<Boolean> retVal = SettableFuture.create();
    final ListenableFuture<FrameFilePartialFetch> clientFuture =
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, StringUtils.format("%s?offset=%d", path, offset))
                .header(HttpHeaders.ACCEPT_ENCODING, "identity"), // Data is compressed at app level
            new FrameFileHttpResponseHandler(channel)
        );

    Futures.addCallback(
        clientFuture,
        new FutureCallback<FrameFilePartialFetch>()
        {
          @Override
          public void onSuccess(FrameFilePartialFetch partialFetch)
          {
            if (partialFetch.isExceptionCaught()) {
              // Exception while reading channel. Recoverable.
              log.noStackTrace().info(
                  partialFetch.getExceptionCaught(),
                  "Encountered exception while reading channel [%s]",
                  channel.getId()
              );
            }

            // Empty fetch means this is the last fetch for the channel.
            partialFetch.backpressureFuture().addListener(
                () -> retVal.set(partialFetch.isLastFetch()),
                Execs.directExecutor()
            );
          }

          @Override
          public void onFailure(Throwable t)
          {
            retVal.setException(t);
          }
        },
        Execs.directExecutor()
    );

    return retVal;
  }

  /**
   * Create a client to communicate with a given worker ID.
   */
  protected abstract ServiceClient getClient(String workerId);

  /**
   * Deserialize a {@link BytesFullResponseHolder} as JSON.
   *
   * It would be reasonable to move this to {@link BytesFullResponseHolder} itself, or some shared utility class.
   */
  protected <T> T deserialize(final BytesFullResponseHolder bytesHolder, final TypeReference<T> typeReference)
  {
    try {
      return objectMapper.readValue(bytesHolder.getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
