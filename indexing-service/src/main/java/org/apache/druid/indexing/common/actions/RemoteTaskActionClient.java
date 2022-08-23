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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final ObjectMapper jsonMapper;
  private final ServiceClient client;

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(
      Task task,
      ServiceClient client,
      ObjectMapper jsonMapper
  )
  {
    this.task = task;
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.debug("Performing action for task[%s]: %s", task.getId(), taskAction);

    try {
      // We're using a ServiceClient directly here instead of OverlordClient, because OverlordClient does
      // not have access to the TaskAction class. (OverlordClient is in the druid-server package, and TaskAction
      // is in the druid-indexing-service package.)
      final Map<String, Object> response = jsonMapper.readValue(
          client.request(
              new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/action")
                  .jsonContent(jsonMapper, new TaskActionHolder(task, taskAction)),
              new BytesFullResponseHandler()
          ).getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      return jsonMapper.convertValue(
          response.get("result"),
          taskAction.getReturnTypeReference()
      );
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      if (e.getCause() instanceof HttpResponseException) {
        // Rewrite the error to be slightly more useful: point out that there may be information in the Overlord logs.
        final StringFullResponseHolder fullResponseHolder = ((HttpResponseException) e.getCause()).getResponse();
        throw new IOE(
            "Error with status[%s] and message[%s]. Check overlord logs for details.",
            fullResponseHolder.getStatus(),
            fullResponseHolder.getContent()
        );
      }

      throw new IOException(e.getCause());
    }
  }
}
