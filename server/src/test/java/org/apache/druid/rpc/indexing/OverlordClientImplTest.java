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

package org.apache.druid.rpc.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class OverlordClientImplTest
{


  @Test
  public void testTaskPayload() throws ExecutionException, InterruptedException, JsonProcessingException
  {
    final String taskID = "taskId_1";
    MockServiceClient client = new MockServiceClient();
    final OverlordClientImpl overlordClient = new OverlordClientImpl(client, DefaultObjectMapper.INSTANCE);

    ClientTaskQuery clientTaskQuery = new ClientKillUnusedSegmentsTaskQuery(taskID, "test", null, null);

    client.expect(new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/task/" + taskID),
                  HttpResponseStatus.OK,
                  Collections.emptyMap(),
                  DefaultObjectMapper.INSTANCE.writeValueAsBytes(new TaskPayloadResponse(taskID, clientTaskQuery))
    );

    Assert.assertEquals(clientTaskQuery, overlordClient.taskPayload(taskID).get().getPayload());
  }
}
