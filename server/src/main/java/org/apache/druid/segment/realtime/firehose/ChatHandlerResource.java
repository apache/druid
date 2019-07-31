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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.initialization.jetty.BadRequestException;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.List;

@Path("/druid/worker/v1/chat")
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

  @Path("/{id}")
  public Object doTaskChat(@PathParam("id") String handlerId, @Context HttpHeaders headers)
  {
    if (taskId != null) {
      final List<String> requestTaskIds = headers.getRequestHeader(TASK_ID_HEADER);
      final String requestTaskId = requestTaskIds != null && !requestTaskIds.isEmpty()
                                   ? Iterables.getOnlyElement(requestTaskIds)
                                   : null;

      // Sanity check: Callers set TASK_ID_HEADER to our taskId (URL-encoded, if >= 0.14.0) if they want to be
      // assured of talking to the correct task, and not just some other task running on the same port.
      if (requestTaskId != null
          && !requestTaskId.equals(taskId)
          && !StringUtils.urlDecode(requestTaskId).equals(taskId)) {
        throw new BadRequestException(
            StringUtils.format("Requested taskId[%s] doesn't match with taskId[%s]", requestTaskId, taskId)
        );
      }
    }

    final Optional<ChatHandler> handler = handlers.get(handlerId);

    if (handler.isPresent()) {
      return handler.get();
    }

    throw new BadRequestException(StringUtils.format("Can't find chatHandler for handler[%s]", handlerId));
  }
}
