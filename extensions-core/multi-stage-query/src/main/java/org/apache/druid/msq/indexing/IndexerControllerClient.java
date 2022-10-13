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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class IndexerControllerClient implements ControllerClient
{
  private final ServiceClient serviceClient;
  private final ObjectMapper jsonMapper;
  private final Closeable baggage;

  public IndexerControllerClient(
      final ServiceClient serviceClient,
      final ObjectMapper jsonMapper,
      final Closeable baggage
  )
  {
    this.serviceClient = serviceClient;
    this.jsonMapper = jsonMapper;
    this.baggage = baggage;
  }

  @Override
  public void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  ) throws IOException
  {
    final String path = StringUtils.format(
        "/keyStatistics/%s/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        workerNumber
    );

    doRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, keyStatistics),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public void postCounters(CounterSnapshotsTree snapshotsTree) throws IOException
  {
    doRequest(
        new RequestBuilder(HttpMethod.POST, "/counters")
            .jsonContent(jsonMapper, snapshotsTree),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public void postResultsComplete(StageId stageId, int workerNumber, @Nullable Object resultObject) throws IOException
  {
    final String path = StringUtils.format(
        "/resultsComplete/%s/%s/%d",
        StringUtils.urlEncode(stageId.getQueryId()),
        stageId.getStageNumber(),
        workerNumber
    );

    doRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, resultObject),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public void postWorkerError(String workerId, MSQErrorReport errorWrapper) throws IOException
  {
    final String path = StringUtils.format(
        "/workerError/%s",
        StringUtils.urlEncode(workerId)
    );

    doRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, errorWrapper),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public void postWorkerWarning(String workerId, List<MSQErrorReport> MSQErrorReports) throws IOException
  {
    final String path = StringUtils.format(
        "/workerWarning/%s",
        StringUtils.urlEncode(workerId)
    );

    doRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, MSQErrorReports),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public List<String> getTaskList() throws IOException
  {
    final BytesFullResponseHolder retVal = doRequest(
        new RequestBuilder(HttpMethod.GET, "/taskList"),
        new BytesFullResponseHandler()
    );

    final MSQTaskList taskList = jsonMapper.readValue(retVal.getContent(), MSQTaskList.class);
    return taskList.getTaskIds();
  }

  @Override
  public void close()
  {
    try {
      baggage.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Similar to {@link ServiceClient#request}, but preserves IOExceptions rather than wrapping them in
   * {@link ExecutionException}.
   */
  private <IntermediateType, FinalType> FinalType doRequest(
      RequestBuilder requestBuilder,
      HttpResponseHandler<IntermediateType, FinalType> handler
  ) throws IOException
  {
    try {
      return FutureUtils.get(serviceClient.asyncRequest(requestBuilder, handler), true);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }
}
