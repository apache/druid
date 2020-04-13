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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;

public class ParallelIndexSupervisorTaskClient extends IndexTaskClient
{
  ParallelIndexSupervisorTaskClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      TaskInfoProvider taskInfoProvider,
      Duration httpTimeout,
      String callerId,
      long numRetries
  )
  {
    super(httpClient, objectMapper, taskInfoProvider, httpTimeout, callerId, 1, numRetries);
  }

  public SegmentIdWithShardSpec allocateSegment(String supervisorTaskId, DateTime timestamp) throws IOException
  {
    final StringFullResponseHolder response = submitSmileRequest(
        supervisorTaskId,
        HttpMethod.POST,
        "segment/allocate",
        null,
        serialize(timestamp),
        true
    );
    if (!isSuccess(response)) {
      throw new ISE(
          "task[%s] failed to allocate a new segment identifier with the HTTP code[%d] and content[%s]",
          supervisorTaskId,
          response.getStatus().getCode(),
          response.getContent()
      );
    } else {
      return deserialize(
          response.getContent(),
          new TypeReference<SegmentIdWithShardSpec>()
          {
          }
      );
    }
  }

  public void report(String supervisorTaskId, SubTaskReport report)
  {
    try {
      final StringFullResponseHolder response = submitSmileRequest(
          supervisorTaskId,
          HttpMethod.POST,
          "report",
          null,
          serialize(report),
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
