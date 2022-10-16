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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.file.FrameFileHttpResponseHandler;
import org.apache.druid.frame.file.FrameFilePartialFetch;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.druid.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexerWorkerClient implements WorkerClient
{
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;

  @GuardedBy("clientMap")
  private final Map<String, Pair<ServiceClient, Closeable>> clientMap = new HashMap<>();

  public IndexerWorkerClient(
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
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
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    return getClient(workerTaskId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/workOrder")
            .jsonContent(jsonMapper, workOrder),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    final String path = StringUtils.format(
        "/resultPartitionBoundaries/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return getClient(workerTaskId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, partitionBoundaries),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  /**
   * Client-side method for {@link WorkerChatHandler#httpPostCleanupStage}.
   */
  @Override
  public ListenableFuture<Void> postCleanupStage(
      final String workerTaskId,
      final StageId stageId
  )
  {
    final String path = StringUtils.format(
        "/cleanupStage/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber()
    );

    return getClient(workerTaskId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, path),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<Void> postFinish(String workerTaskId)
  {
    return getClient(workerTaskId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/finish"),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String workerTaskId)
  {
    return FutureUtils.transform(
        getClient(workerTaskId).asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/counters"),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<CounterSnapshotsTree>() {})
    );
  }

  private static final Logger log = new Logger(IndexerWorkerClient.class);

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  )
  {
    final ServiceClient client = getClient(workerTaskId);
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
        }
    );

    return retVal;
  }

  @Override
  public void close() throws IOException
  {
    synchronized (clientMap) {
      try {
        final List<Closeable> closeables =
            clientMap.values().stream().map(pair -> pair.rhs).collect(Collectors.toList());
        CloseableUtils.closeAll(closeables);
      }
      finally {
        clientMap.clear();
      }
    }
  }

  private ServiceClient getClient(final String workerTaskId)
  {
    synchronized (clientMap) {
      return clientMap.computeIfAbsent(
          workerTaskId,
          id -> {
            final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(id, overlordClient);
            final ServiceClient client = clientFactory.makeClient(
                id,
                locator,
                new SpecificTaskRetryPolicy(workerTaskId, StandardRetryPolicy.unlimited())
            );
            return Pair.of(client, locator);
          }
      ).lhs;
    }
  }

  /**
   * Deserialize a {@link BytesFullResponseHolder} as JSON.
   *
   * It would be reasonable to move this to {@link BytesFullResponseHolder} itself, or some shared utility class.
   */
  private <T> T deserialize(final BytesFullResponseHolder bytesHolder, final TypeReference<T> typeReference)
  {
    try {
      return jsonMapper.readValue(bytesHolder.getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
