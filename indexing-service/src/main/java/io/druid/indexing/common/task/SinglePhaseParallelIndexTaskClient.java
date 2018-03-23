/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.IndexTaskClient;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.java.util.common.ISE;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.List;

public class SinglePhaseParallelIndexTaskClient extends IndexTaskClient
{
  private final String subtaskId;

  public SinglePhaseParallelIndexTaskClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      TaskInfoProvider taskInfoProvider,
      Duration httpTimeout,
      String callerId,
      long numRetries
  )
  {
    super(httpClient, objectMapper, taskInfoProvider, httpTimeout, callerId, 1, numRetries);
    this.subtaskId = callerId;
  }

  public String getSubtaskId()
  {
    return subtaskId;
  }

  public void report(String supervisorTaskId, List<DataSegment> pushedSegments)
  {
    try {
      final FullResponseHolder response = submitSmilRequest(
          supervisorTaskId,
          HttpMethod.POST,
          "report",
          null,
          serialize(new PushedSegmentsReport(subtaskId, pushedSegments)),
          true
      );
      if (!isSuccess(response)) {
        throw new ISE(
            "Failed to send taskReports to task[%s] with the HTTP code [%d]",
            supervisorTaskId,
            response.getStatus().getCode()
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
