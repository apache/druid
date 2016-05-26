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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.druid.server.metrics.DataSourceTaskIdHolder;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.List;

@Path("/druid/worker/v1")
public class ChatHandlerResource
{
  public static final String TASK_ID_HEADER = "X-Druid-Task-Id";

  private final ChatHandlerProvider handlers;
  private final String taskId;

  @Inject
  public ChatHandlerResource(final ChatHandlerProvider handlers, final DataSourceTaskIdHolder taskIdHolder)
  {
    this.handlers = handlers;
    this.taskId = taskIdHolder.getTaskId();
  }

  @Path("/chat/{id}")
  public Object doTaskChat(@PathParam("id") String handlerId, @Context HttpHeaders headers)
  {
    if (taskId != null) {
      List<String> requestTaskId = headers.getRequestHeader(TASK_ID_HEADER);
      if (requestTaskId != null && !requestTaskId.contains(taskId)) {
        return null;
      }
    }

    final Optional<ChatHandler> handler = handlers.get(handlerId);

    if (handler.isPresent()) {
      return handler.get();
    }

    return null;
  }
}
