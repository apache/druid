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

package org.apache.druid.messages.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import java.util.Collections;

public class MessageRelayClientImplTest
{
  private ObjectMapper smileMapper;
  private MockServiceClient serviceClient;
  private MessageRelayClient<String> messageRelayClient;

  @Before
  public void setup()
  {
    smileMapper = new DefaultObjectMapper(new SmileFactory(), null);
    serviceClient = new MockServiceClient();
    messageRelayClient = new MessageRelayClientImpl<>(serviceClient, smileMapper, String.class);
  }

  @After
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void test_getMessages_ok() throws Exception
  {
    final MessageBatch<String> batch = new MessageBatch<>(ImmutableList.of("foo", "bar"), 123, 0);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/outbox/me/messages?epoch=-1&watermark=0"),
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
        smileMapper.writeValueAsBytes(batch)
    );

    final ListenableFuture<MessageBatch<String>> result = messageRelayClient.getMessages("me", MessageRelay.INIT, 0);
    Assert.assertEquals(batch, result.get());
  }

  @Test
  public void test_getMessages_noContent() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/outbox/me/messages?epoch=-1&watermark=0"),
        HttpResponseStatus.NO_CONTENT,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
        ByteArrays.EMPTY_ARRAY
    );

    final ListenableFuture<MessageBatch<String>> result = messageRelayClient.getMessages("me", MessageRelay.INIT, 0);
    Assert.assertEquals(new MessageBatch<>(Collections.emptyList(), MessageRelay.INIT, 0), result.get());
  }
}
